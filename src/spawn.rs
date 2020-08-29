use failure::Error;

use crate::fd::{Fd, RawFd};

#[cfg(unix)]
mod imp {
    use super::*;
    use nix::fcntl::{fcntl, FcntlArg, FdFlag};
    use nix::sys::signal::{kill, Signal, SIGHUP, SIGTERM};
    use nix::unistd::{getpid, Pid};
    use std::convert::TryInto;
    use tokio::process::{Child, Command};
    use tokio::signal::unix::{signal, SignalKind};
    use tokio::stream::StreamExt;

    fn share_listeners(raw_fds: &[(Fd, RawFd)]) -> Result<(), Error> {
        for &(_, raw_fd) in raw_fds {
            let flags = fcntl(raw_fd, FcntlArg::F_GETFD)?;
            let mut fdflags = FdFlag::from_bits_truncate(flags);
            fdflags.remove(FdFlag::FD_CLOEXEC);
            fcntl(raw_fd, FcntlArg::F_SETFD(fdflags))?;
        }
        Ok(())
    }

    fn spawn_child(
        raw_fds: &[(Fd, RawFd)],
        cmdline: &[&str],
        no_pid: bool,
    ) -> Result<Child, Error> {
        let mut cmd = Command::new(&cmdline[0]);
        cmd.args(&cmdline[1..]);

        if !raw_fds.is_empty() {
            cmd.env("LISTEN_FDS", raw_fds.len().to_string());
            if !no_pid {
                cmd.env("LISTEN_PID", getpid().to_string());
            }
        }
        cmd.spawn().map_err(Error::from)
    }

    fn send_signal(child: &Child, sig: Signal) -> Result<(), Error> {
        kill(Pid::from_raw(child.id().try_into().unwrap()), sig).map_err(Error::from)
    }

    pub fn spawn(raw_fds: Vec<(Fd, RawFd)>, cmdline: &[&str], no_pid: bool) -> Result<(), Error> {
        share_listeners(&raw_fds)?;

        tokio::runtime::Builder::new()
            .threaded_scheduler()
            .core_threads(num_cpus::get())
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let mut child = spawn_child(&raw_fds, cmdline, no_pid).expect("failed to create child process");

                let mut hangup_stream = signal(SignalKind::hangup()).expect("cannot get signal hangup");
                let mut terminate_stream =
                    signal(SignalKind::terminate()).expect("cannot get signal terminal");
                let mut user_defined2_stream =
                    signal(SignalKind::user_defined2()).expect("cannot get signal user_defined2");

                loop {
                    tokio::select! {
                        _ = hangup_stream.next() => {
                            println!("got signal HUP");
                            send_signal(&child, SIGHUP).expect("send SIGHUP to child");
                        }
                        _ = terminate_stream.next() => {
                            println!("got signal TERM");
                            send_signal(&child, SIGTERM).expect("send SIGTERM to child");
                            break;
                        }
                        _ = user_defined2_stream.next() => {
                            println!("got signal USR2");
                            let new_child = spawn_child(&raw_fds, cmdline, no_pid).expect("failed to create new child process");
                            send_signal(&child, SIGTERM).expect("send SIGTERM to old child");
                            let status = child.await.expect("child process status");
                            println!("child process exit status={}", status);
                            child = new_child;
                        }
                    }
                }
            });
        Ok(())
    }
}

#[cfg(windows)]
mod imp {
    use super::*;
    use std::io::{Read, Write};
    use std::mem;
    use std::net::{TcpListener, TcpStream};
    use std::process::Command;
    use std::slice;
    use std::thread;

    use failure::err_msg;
    use uuid::Uuid;
    use winapi::shared::minwindef::DWORD;
    use winapi::um::winsock2::{WSADuplicateSocketW, SOCKET, WSAPROTOCOL_INFOW};

    use utils::QuietExit;

    fn share_sockets(
        mut sock: TcpStream,
        ref_secret: &Uuid,
        raw_fds: &[(Fd, RawFd)],
    ) -> Result<(), Error> {
        let mut data = Vec::new();
        sock.read_to_end(&mut data)?;
        let out = String::from_utf8(data)?;
        let mut pieces = out.split("|");

        let secret: Uuid = pieces
            .next()
            .and_then(|x| x.parse().ok())
            .ok_or_else(|| err_msg("invalid secret"))?;
        if &secret != ref_secret {
            return Err(err_msg("invalid secret"));
        }
        let pid: DWORD = pieces
            .next()
            .and_then(|x| x.parse().ok())
            .ok_or_else(|| err_msg("invalid or missing pid"))?;

        for &(_, raw_fd) in raw_fds {
            let mut proto_info: WSAPROTOCOL_INFOW = unsafe { mem::zeroed() };
            unsafe {
                let rv = WSADuplicateSocketW(raw_fd as SOCKET, pid, &mut proto_info);
                if rv != 0 {
                    return Err(err_msg(format!("socket duplicate failed with {}", rv)));
                }
            }
            let bytes: *const u8 = unsafe { mem::transmute(&proto_info) };
            sock.write_all(unsafe {
                slice::from_raw_parts(bytes, mem::size_of::<WSAPROTOCOL_INFOW>())
            })?;
        }

        Ok(())
    }

    pub fn spawn(raw_fds: Vec<(Fd, RawFd)>, cmdline: &[&str], _no_pid: bool) -> Result<(), Error> {
        let mut cmd = Command::new(&cmdline[0]);
        cmd.args(&cmdline[1..]);

        let secret: Uuid = Uuid::new_v4();
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let sockserver_addr = listener.local_addr()?;

        cmd.env("SYSTEMFD_SOCKET_SERVER", sockserver_addr.to_string());
        cmd.env("SYSTEMFD_SOCKET_SECRET", secret.to_string());

        thread::spawn(move || {
            for stream in listener.incoming() {
                if let Ok(stream) = stream {
                    share_sockets(stream, &secret, &raw_fds).unwrap();
                }
            }
        });

        let mut child = cmd.spawn()?;
        let status = child.wait()?;

        Err(QuietExit(status.code().unwrap()).into())
    }
}

pub use self::imp::*;

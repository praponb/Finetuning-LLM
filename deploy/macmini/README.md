# sql2pyspark.praponb.com on the Mac mini

Since 2026-11-08 the public site runs on the Mac mini M6 (32 GB) instead of the Ubuntu server: about 3 s per
conversion instead of 35–50 s. Three LaunchDaemons start at boot (after the FileVault unlock), without a GUI login,
and run as `praponb`:

| Daemon | What | Port |
| --- | --- | --- |
| `com.praponb.ollama` | `ollama serve` (models in `~/.ollama`), `OLLAMA_NUM_PARALLEL=1` (one request at a time) | 127.0.0.1:11434 |
| `com.praponb.sql2pyspark` | Streamlit `app.py` from `~/GitHub/Finetuning-LLM`, the project venv | 127.0.0.1:8502 |
| `com.praponb.cloudflared-sql2pyspark` | Cloudflare Tunnel `sql2pyspark` (`1f233bc8…`), config `~/.cloudflared/sql2pyspark-config.yml` | — |

Cloudflare Access (application `sql2pyspark`, policy *Owner only*) is tied to the hostname and is unchanged.
`make app` keeps port 8501 for local work.

## Install

1. Quit the Ollama app and remove it from **System Settings → General → Login Items → Open at Login** (the daemon
   replaces it; keep it allowed under *Allow in the Background*). Don't open `Ollama.app` again: it takes port 11434
   while the daemon is down and keeps it. That happened from 2026-11-17 to 11-26, when the site ran on the app's own
   settings instead of the daemon's.
2. `sudo zsh deploy/macmini/install.sh` — copies the plists to `/Library/LaunchDaemons`, loads them, sets
   `pmset sleep 0 autorestart 1`, and checks Ollama and the app.

## Operate

```bash
sudo launchctl print system/com.praponb.sql2pyspark | grep -E 'state|pid'   # status (same for the other two)
sudo launchctl kickstart -k system/com.praponb.sql2pyspark                 # restart, e.g. after a code change
tail -f ~/Library/Logs/sql2pyspark/{app,ollama,cloudflared}.log             # logs
cloudflared tunnel info sql2pyspark                                         # one darwin_arm64 connector
lsof -nP -iTCP:11434 -sTCP:LISTEN                                          # must be /opt/homebrew/bin/ollama, not Ollama.app
pkill -f "streamlit run app.py --server.port 8502"                          # restart the app without sudo (KeepAlive)
```

- **New model**: `ollama cp sql2pyspark:<tag> sql2pyspark:latest` (rollback: copy the previous tag back). The app
  always asks for `latest`. `deploy/update_model.sh` is only for the old server.
- **Code change**: pull/merge into `~/GitHub/Finetuning-LLM`, then `kickstart -k` the app.
- Training on this Mac shares the GPU with the site; the training chain only unloads models, Ollama keeps running.

## Uninstall

```bash
for L in com.praponb.ollama com.praponb.sql2pyspark com.praponb.cloudflared-sql2pyspark; do
  sudo launchctl bootout system/$L; sudo rm /Library/LaunchDaemons/$L.plist; done
```

## Rollback

The Mac mini is the only host. To go back to the previous model:
`ollama cp sql2pyspark:q9df3-q4 sql2pyspark:latest` (the app always asks for `latest`; every tag from v8 on is kept on
the Mac mini; `latest` = q9gm-q4 since 2026-11-27, q9df3-q4 2026-11-25 → 11-27, v18a-q4 before). For a code change, `git checkout` the earlier commit and
`sudo launchctl kickstart -k system/com.praponb.sql2pyspark`.

The Ubuntu server is no longer a fallback: its copy (`sql2pyspark.service`, the `sql2pyspark:*` Ollama tags, `~/web-env`,
`~/Finetuning-LLM` and the `config.yml.bak-20261108-sql2pyspark` backup) was removed on 2026-11-14. To host the site
there again, set it up from scratch with [docs/08](../../docs/08-server-deployment.md).

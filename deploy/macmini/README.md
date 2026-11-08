# sql2pyspark.praponb.com on the Mac mini

Since 2026-11-08 the public site runs on the Mac mini M6 (32 GB) instead of the Ubuntu server: about 3 s per
conversion instead of 35–50 s. Three LaunchDaemons start at boot (after the FileVault unlock), without a GUI login,
and run as `praponb`:

| Daemon | What | Port |
| --- | --- | --- |
| `com.praponb.ollama` | `ollama serve` (models in `~/.ollama`) | 127.0.0.1:11434 |
| `com.praponb.sql2pyspark` | Streamlit `app.py` from `~/GitHub/Finetuning-LLM`, the project venv | 127.0.0.1:8502 |
| `com.praponb.cloudflared-sql2pyspark` | Cloudflare Tunnel `sql2pyspark` (`1f233bc8…`), config `~/.cloudflared/sql2pyspark-config.yml` | — |

Cloudflare Access (application `sql2pyspark`, policy *Owner only*) is tied to the hostname and is unchanged.
`make app` keeps port 8501 for local work.

## Install

1. Quit the Ollama app and remove it from **System Settings → General → Login Items** (the daemon replaces it).
2. `sudo zsh deploy/macmini/install.sh` — copies the plists to `/Library/LaunchDaemons`, loads them, sets
   `pmset sleep 0 autorestart 1`, and checks Ollama and the app.

## Operate

```bash
sudo launchctl print system/com.praponb.sql2pyspark | grep -E 'state|pid'   # status (same for the other two)
sudo launchctl kickstart -k system/com.praponb.sql2pyspark                 # restart, e.g. after a code change
tail -f ~/Library/Logs/sql2pyspark/{app,ollama,cloudflared}.log             # logs
cloudflared tunnel info sql2pyspark                                         # one darwin_arm64 connector
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

## Rollback to the Ubuntu server

On the server (`ssh prapon@192.168.1.49`): restore the backed-up `/etc/cloudflared/config.yml` (it has the
`sql2pyspark.praponb.com → http://localhost:8501` rule), `sudo systemctl restart cloudflared`,
`sudo systemctl enable --now sql2pyspark`. Then on the Mac mini:
`cloudflared tunnel route dns --overwrite-dns inventory-tunnel sql2pyspark.praponb.com`.

# 08 - Server Deployment (Mac mini; formerly Ubuntu + Cloudflare Tunnel)

## 🍎 Current: the Mac mini M6 (since 2026-11-08)

**<https://sql2pyspark.praponb.com>** is served from the Mac mini M6 (32 GB), the same machine that trains the model.
Three LaunchDaemons start at boot (after the FileVault unlock) without a GUI login and run as `praponb`:

| Daemon | What |
| :--- | :--- |
| `com.praponb.ollama` | `ollama serve` on 127.0.0.1:11434 (models in `~/.ollama`; `OLLAMA_NUM_PARALLEL=1` since 2026-11-26; replaces the Ollama app, which is no longer a Login Item) |
| `com.praponb.sql2pyspark` | Streamlit `app.py` on **127.0.0.1:8502** from `~/GitHub/Finetuning-LLM` with the project venv (`make app` keeps 8501) |
| `com.praponb.cloudflared-sql2pyspark` | Cloudflare Tunnel **`sql2pyspark`** (`1f233bc8…`), config `~/.cloudflared/sql2pyspark-config.yml` |

- **Speed**: the Metal GPU answers the default GROUP BY query in ~3.5 s including the model load (the server: 35–50 s).
- **Login**: the same Cloudflare Access application (it is tied to the hostname, so the move didn't change it).
- **Kimi repair**: on, with **Kimi K3.1 Pro** (`KIMI_MODEL=kimi-k3.1-pro` in the repo's `.env`).
- **New model**: `ollama cp sql2pyspark:<tag> sql2pyspark:latest` on the Mac mini; `deploy/update_model.sh` is not needed.
- **Code change**: update `~/GitHub/Finetuning-LLM`, then `sudo launchctl kickstart -k system/com.praponb.sql2pyspark`.
- **Ollama app incident (2026-11-17 → 11-26)**: the daemon was unloaded on 11-17 and `Ollama.app` took port 11434, so
  the site ran on the app's settings. Fixed on 2026-11-26 (Bangkok time): `OLLAMA_NUM_PARALLEL=1` added to
  `com.praponb.ollama.plist` and reinstalled, the app quit and removed from *Open at Login*, and the daemon owns the port
  again. Don't open `Ollama.app` on the Mac mini.
- **Power**: `pmset sleep 0 autorestart 1`; after a power cut the Mac boots but waits for the FileVault password.
- Install, operation, logs and rollback: [deploy/macmini/README.md](../deploy/macmini/README.md).

**The switch (2026-11-08)**: the daemons were installed and checked first (`sudo zsh deploy/macmini/install.sh`), then
`cloudflared tunnel route dns --overwrite-dns sql2pyspark sql2pyspark.praponb.com` moved the hostname at 14:31:53
(Bangkok time) without downtime. On the server, `deploy/macmini/server_stop_sql2pyspark.sh` disabled `sql2pyspark.service` and
removed the hostname's rule from `inventory-tunnel` (backup `/etc/cloudflared/config.yml.bak-20261108-sql2pyspark`).
The server's copy was kept for a rollback until **2026-11-14**, when it was removed (the unit and its backups, the `sql2pyspark:*` Ollama tags, `~/web-env`, `~/Finetuning-LLM`, `~/sql2pyspark.gguf` and the cloudflared config backup, plus the Docker Compose copy from 2026-09-29: containers `finetuning-llm-app-1` / `finetuning-llm-ollama-1` with v7 on LAN ports 8501 / 11435, their images and the `finetuning-llm_ollama-models` volume); the server's Ollama still serves `batch2stream`. Rollback is now a tag on the Mac mini ([deploy/macmini](../deploy/macmini/README.md#rollback)).

## 🗄️ History: the Ubuntu server (2026-09-29 → 2026-11-08)

Training needs Apple MLX, so it stays on the Mac. The finished model is a plain GGUF file that Ollama can serve on any Linux machine. The rest of this guide covers the deployment of **<https://sql2pyspark.praponb.com>** on a home Ubuntu server, set up on 2026-09-29 and used until 2026-11-08; the server copy was removed on 2026-11-14, so these steps now describe a fresh setup.

---

## 🖥️ The server

| Item | Value |
| :--- | :--- |
| Host | `192.168.1.49` (LAN only), HP EliteDesk 705 G5 Desktop Mini, Ubuntu |
| CPU / RAM / disk | 8 threads, 13 GB RAM (~11 GB free), 364 GB free; **no NVIDIA GPU** |
| Model | Native Ollama (`ollama.service`), `sql2pyspark:latest` = **v15-q4** (`fa40b31b8b20` on the server's Ollama 0.34.4, same weights file as the Mac's `882787d22c3c`; 4.7 GB Q4_K_M, since 2026-10-09 06:29); v12-q4 (`3a8645cee5be`, 2026-10-03 → 10-09), v9-q4 (`e35022eac39a`, 2026-10-01 → 10-03), v8 and v7 kept for rollback |
| App | `sql2pyspark.service` (systemd, Streamlit on port 8501, venv `~/web-env`), code in `~/Finetuning-LLM` |
| Public access | Cloudflare Tunnel (`cloudflared.service`): `sql2pyspark.praponb.com` → `http://localhost:8501` |
| Login | Cloudflare Access (since 2026-09-30): application `sql2pyspark`, policy *Owner only*, Cloudflare or one-time PIN login, 24 h session |

**Speed**: without a GPU the 7B model runs on the CPU. v7's default query took **69 s** end to end in the app; a short GROUP BY query took **17.8 s** straight from Ollama with v8 (on the M2 Mac: ~5 s). Long SQL (thousands of characters) can take several minutes and may exceed the app's 600 s timeout. The Q4_K_M build (v9-q4, 4.68 GB) is clearly faster than Q8_0 here: same prompt (the app's default GROUP BY query), same 94-token answer, measured 2026-10-01:

| Model | Load | Generation | Total (warm) |
| :--- | :--- | :--- | :--- |
| v8 (Q8_0, 8.1 GB) | 36 s | 2.47 tokens/s | 38.5 s |
| **v9-q4 (Q4_K_M, 4.7 GB)** | 16 s | **4.05 tokens/s** | **23.5 s** |

## 📏 Sizing

| Resource | Needed for v7/v8 (7B, Q8_0) | Needed for v9-q4 (7B, Q4_K_M) |
| :--- | :--- | :--- |
| RAM | Ollama ~8 GB (7.9 GB measured with a 4k context), app ~0.3 GB (up to ~1.5 GB while Spark runs a Kimi check), OS ~1–1.5 GB → **12 GB minimum, 16 GB recommended**. v5 (3B) fits in 8 GB. | Ollama ~4.8 GB, app ~0.3–1.5 GB, OS ~1–1.5 GB → **8 GB minimum, 12 GB recommended**. |
| Disk | GGUF 8.1 GB + Ollama's own copy 8.1 GB + packages/images ~5–7 GB → **~25 GB; plan 50 GB free** (room for the next model during an update) | GGUF 4.7 GB + Ollama copy 4.7 GB → **~15 GB; plan 30 GB free**. |
| GPU | Optional. With an NVIDIA GPU (≥ 12 GB VRAM) answers are several times faster. On a Mac, containers can't use the Apple GPU: there Ollama runs natively (see [Serving](05-serving-and-streamlit-ui.md)). | Optional. On this CPU 1.64× the generation speed of Q8_0 (4.05 vs. 2.47 tokens/s). |

## 🚀 Setup steps

1. **Ollama and the model**: install Ollama (`curl -fsSL https://ollama.com/install.sh | sh`), copy `sql2pyspark.gguf` and `Modelfile` from the Mac (`rsync -avP sql2pyspark.gguf Modelfile prapon@192.168.1.49:~/`) and register it: `ollama create sql2pyspark -f Modelfile` (in the folder with both files).
2. **Code**: `git clone https://github.com/praponb/Finetuning-LLM.git ~/Finetuning-LLM` (private repo: authenticate first, e.g. `gh auth login`). Put `.env` there only if the Kimi K3 repair should be available (see Security).
3. **Python environment**: `python3 -m venv ~/web-env && ~/web-env/bin/pip install streamlit==1.64.0 requests==2.34.2 openai==3.19.2 pyspark==4.2.0` (Ubuntu 24.04 blocks a system-wide `pip install`).
4. **Service**: run [`deploy/setup_server.sh`](../deploy/setup_server.sh) once with `sudo`. It installs Java 17 (for the Kimi checks' Spark), backs up `/etc/systemd/system/sql2pyspark.service`, points it at `~/Finetuning-LLM`, adds `Environment=KIMI_API_KEY=` (Kimi off), restarts and checks the app. The resulting unit is [`deploy/sql2pyspark.service`](../deploy/sql2pyspark.service); on a fresh server, copy it to `/etc/systemd/system/` and `sudo systemctl enable --now sql2pyspark`.
5. **Cloudflare Tunnel** (no router changes; `praponb.com` DNS must be on Cloudflare):

   ```bash
   sudo mkdir -p --mode=0755 /usr/share/keyrings
   curl -fsSL https://pkg.cloudflare.com/cloudflare-main.gpg | sudo tee /usr/share/keyrings/cloudflare-main.gpg >/dev/null
   echo 'deb [signed-by=/usr/share/keyrings/cloudflare-main.gpg] https://pkg.cloudflare.com/cloudflared any main' | sudo tee /etc/apt/sources.list.d/cloudflared.list
   sudo apt-get update && sudo apt-get install cloudflared
   ```

   Then in **Zero Trust → Networks → Tunnels → Create a tunnel → Cloudflared**, run the shown `sudo cloudflared service install <TOKEN>` on the server and add the public hostname `sql2pyspark.praponb.com` → Type **HTTP**, URL `localhost:8501` (or `127.0.0.1:8501`).
6. **Login** (required before enabling Kimi; done on 2026-09-30): in Cloudflare One, **Access controls → Applications → Create new application → Self-hosted and private**, public hostname `sql2pyspark` + `praponb.com`, **Create new policy** (*Allow*, Include → *Emails* → your address), keep *Accept all available identity providers*, **Create**. Then **Integrations → Identity providers → Add → One-time PIN**, so the login page also offers an emailed code (otherwise only *Sign in with Cloudflare*, which needs a Cloudflare account). Check: an unauthenticated `curl -I https://sql2pyspark.praponb.com/` must return `302` to `<team>.cloudflareaccess.com`.

## 🔒 Security

- The app has no login of its own; Cloudflare Access provides it (only emails in the *Owner only* policy get through). With a key, the Kimi K3 repair spends money and sends the user's SQL and code to Moonshot's API, so the service keeps it off (`Environment=KIMI_API_KEY=`, an empty value that wins over `.env`). Enable it only behind a login: delete that line, then `sudo systemctl daemon-reload && sudo systemctl restart sql2pyspark`.
- Ollama listens on `localhost:11434` only; the tunnel needs no inbound ports (`sudo ufw allow OpenSSH && sudo ufw enable` is safe).

## 🔄 Updates

- **Code**: the server has no GitHub credentials for the private repo, so `git pull` fails there (`could not read Username`). Push from the Mac instead, then fast-forward and restart on the server:

  ```bash
  git push prapon@192.168.1.49:Finetuning-LLM origin/main:refs/heads/main-from-mac      # on the Mac
  ssh prapon@192.168.1.49 'cd ~/Finetuning-LLM && git merge --ff-only main-from-mac && git branch -D main-from-mac'
  ssh -t prapon@192.168.1.49 'sudo systemctl restart sql2pyspark'
  ```

- **Model** (after a new fine-tune, `make export` and `make promote` on the Mac; **run the script on the Mac**, from the repo root — it copies the file with `scp` and runs the Ollama commands on the server over `ssh`): `zsh deploy/update_model.sh v8 v7` copies the Mac's `sql2pyspark.gguf` to the server (`scp`, 8.1 GB), keeps the server's current `latest` as `sql2pyspark:v7`, registers the new file as `sql2pyspark:v8` (same `Modelfile` as the repo) and points `latest` at it; no sudo and no app restart needed. To deploy the 4-bit quantized model instead:

  ```bash
  GGUF=sql2pyspark-q4.gguf zsh deploy/update_model.sh v9-q4 v8
  ```

  `sql2pyspark-q4.gguf` on the Mac holds **v15-q4** (exported 2026-10-07 21:27, restored after the v16 export, which is not used); v15-q4 is `latest` on the Mac since 2026-10-09 05:50 and was deployed to the server with `GGUF=sql2pyspark-q4.gguf zsh deploy/update_model.sh v15-q4 v12-q4` (2026-10-09 06:29; v12-q4 before, deployed 2026-10-03 16:48 with `... update_model.sh v12-q4 v9-q4`); v10-q4 and v11-q4 are Ollama tags only now (re-export from `classic_v10/` or `targeted_v11/adapters` to deploy one). **2026-10-02**: the server's code was fast-forwarded to `main` at the PR #19 merge (`05707cd`, push from the Mac; no app code changed, no restart); its model stayed v9-q4 until 2026-10-03; v9-q4 now exists on the Mac only as an Ollama tag (to get its GGUF file again, re-export it from `complex_v9/adapters`). The server keeps v9-q4, so rollback there needs no file: `ssh prapon@192.168.1.49 'ollama cp sql2pyspark:v9-q4 sql2pyspark:latest'`.
  Rollback: `ssh prapon@192.168.1.49 'ollama cp sql2pyspark:v8 sql2pyspark:latest'`.
- **Status 2026-09-30**: the setup script has been run (Java 17, `WorkingDirectory=/home/prapon/Finetuning-LLM`, Kimi off); **Cloudflare Access is active** since 2026-09-30: Zero Trust team `odd-shape-7d9a`, self-hosted application `sql2pyspark` for `sql2pyspark.praponb.com` with the Allow policy *Owner only* (emails: the owner's address), session 24 h, login methods *Cloudflare* and *One-time PIN* (email code); unauthenticated requests get a 302 to `odd-shape-7d9a.cloudflareaccess.com`. To let someone else in, add their email to the *Owner only* policy (Access controls → Policies). Kimi can now be enabled on the server: remove the `Environment=KIMI_API_KEY=` line from the unit, `sudo systemctl daemon-reload && sudo systemctl restart sql2pyspark`. v8 was deployed with `deploy/update_model.sh v8 v7` on 2026-09-30 (same model ID as the Mac; a GROUP BY test query took 17.8 s on the CPU). **2026-10-01**: v9-q4 deployed with `GGUF=sql2pyspark-q4.gguf zsh deploy/update_model.sh v9-q4 v8` (same GGUF checksum as the Mac; the Ollama ID differs only because the server runs Ollama 0.34.4), and the code was fast-forwarded to `main` at the PR #16 merge (`9ca22fa`). **2026-10-03 16:48**: v12-q4 deployed with `GGUF=sql2pyspark-q4.gguf zsh deploy/update_model.sh v12-q4 v9-q4` (same weights blob as the Mac's v12-q4); the server's code stays at `3e58dd6` (only docs changed on `main` since). **2026-10-09 06:29**: v15-q4 deployed with `GGUF=sql2pyspark-q4.gguf zsh deploy/update_model.sh v15-q4 v12-q4` (the copy took 26 min at ~3 MB/s; same SHA-256 as the Mac's file); test conversion (GROUP BY + HAVING, 95 tokens) correct at 4.0 tokens/s, 16.7 s model load, 48.6 s cold; v12-q4 kept, rollback `ssh prapon@192.168.1.49 'ollama cp sql2pyspark:v12-q4 sql2pyspark:latest'`; the server's code still needs no update (no app code changed).

## 🩺 Troubleshooting (seen during the setup)

| Symptom | Cause | Fix |
| :--- | :--- | :--- |
| Cloudflare **502 Bad gateway** (Host: Error) | The tunnel works but `cloudflared` gets no answer from the app | Check `curl http://127.0.0.1:8501/_stcore/health` on the server; the hostname's service must be Type **HTTP** (not HTTPS); try `127.0.0.1:8501` instead of `localhost` |
| Cloudflare **Error 1033** | No running connector for the tunnel | `sudo systemctl enable --now cloudflared`; after re-creating a tunnel, reinstall the service with the new token and re-add the hostname |
| `ModuleNotFoundError: No module named 'kimi_repair'` | The service ran a copied `~/app.py` outside the repo | Run the app from `~/Finetuning-LLM` (step 4); the interim fix was a symlink `~/kimi_repair.py -> ~/Finetuning-LLM/kimi_repair.py` |
| Conversion times out on long SQL | CPU-only 7B is slow on Q8_0 | Use the Q4_K_M build (v9-q4, 1.64× faster than Q8_0 on this CPU), a GPU, serve v5 (3B), or raise the timeouts in `app.py` |

The Docker alternative (`OLLAMA_URL=http://ollama:11434 docker compose --profile ollama up -d --build`, see the [Operations Runbook](07-operations-runbook.md#-docker)) also works on Linux; this server uses native Ollama and systemd instead.

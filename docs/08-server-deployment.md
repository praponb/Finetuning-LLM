# 08 - Server Deployment (Ubuntu + Cloudflare Tunnel)

Training needs Apple MLX, so it stays on the Mac. The finished model is a plain GGUF file that Ollama can serve on any Linux machine. This guide covers the deployment of **https://sql2pyspark.praponb.com** on a home Ubuntu server, set up on 2026-09-29.

---

## 🖥️ The server

| Item | Value |
| :--- | :--- |
| Host | `192.168.1.49` (LAN only), HP EliteDesk 705 G5 Desktop Mini, Ubuntu |
| CPU / RAM / disk | 8 threads, 13 GB RAM (~11 GB free), 364 GB free; **no NVIDIA GPU** |
| Model | Native Ollama (`ollama.service`), `sql2pyspark:latest` = **v9-q4** (`e35022eac39a` on the server's Ollama 0.34.4, same weights blob as the Mac's `f19cf7096ebb`; 4.7 GB Q4_K_M, since 2026-10-01); v8 and v7 kept for rollback |
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
- **Model** (after a new fine-tune, `make export` and `make promote` on the Mac): `zsh deploy/update_model.sh v8 v7` copies the Mac's `sql2pyspark.gguf` to the server (`scp`, 8.1 GB), keeps the server's current `latest` as `sql2pyspark:v7`, registers the new file as `sql2pyspark:v8` (same `Modelfile` as the repo) and points `latest` at it; no sudo and no app restart needed. To deploy the 4-bit quantized model instead:
  ```bash
  GGUF=sql2pyspark-q4.gguf zsh deploy/update_model.sh v9-q4 v8
  ```
  Since 2026-10-01 18:44 `sql2pyspark-q4.gguf` on the Mac holds **v10-q4** (the candidate), so the next deployment, if the owner promotes it, is `GGUF=sql2pyspark-q4.gguf zsh deploy/update_model.sh v10-q4 v9-q4`; v9-q4 now exists on the Mac only as an Ollama tag (to get its GGUF file again, re-export it from `complex_v9/adapters`). The server already has v9-q4, so rollback there needs no file.
  Rollback: `ssh prapon@192.168.1.49 'ollama cp sql2pyspark:v8 sql2pyspark:latest'`.
- **Status 2026-09-30**: the setup script has been run (Java 17, `WorkingDirectory=/home/prapon/Finetuning-LLM`, Kimi off); **Cloudflare Access is active** since 2026-09-30: Zero Trust team `odd-shape-7d9a`, self-hosted application `sql2pyspark` for `sql2pyspark.praponb.com` with the Allow policy *Owner only* (emails: the owner's address), session 24 h, login methods *Cloudflare* and *One-time PIN* (email code); unauthenticated requests get a 302 to `odd-shape-7d9a.cloudflareaccess.com`. To let someone else in, add their email to the *Owner only* policy (Access controls → Policies). Kimi can now be enabled on the server: remove the `Environment=KIMI_API_KEY=` line from the unit, `sudo systemctl daemon-reload && sudo systemctl restart sql2pyspark`. v8 was deployed with `deploy/update_model.sh v8 v7` on 2026-09-30 (same model ID as the Mac; a GROUP BY test query took 17.8 s on the CPU). **2026-10-01**: v9-q4 deployed with `GGUF=sql2pyspark-q4.gguf zsh deploy/update_model.sh v9-q4 v8` (same GGUF checksum as the Mac; the Ollama ID differs only because the server runs Ollama 0.34.4), and the code was fast-forwarded to `main` at the PR #16 merge (`9ca22fa`).

## 🩺 Troubleshooting (seen during the setup)

| Symptom | Cause | Fix |
| :--- | :--- | :--- |
| Cloudflare **502 Bad gateway** (Host: Error) | The tunnel works but `cloudflared` gets no answer from the app | Check `curl http://127.0.0.1:8501/_stcore/health` on the server; the hostname's service must be Type **HTTP** (not HTTPS); try `127.0.0.1:8501` instead of `localhost` |
| Cloudflare **Error 1033** | No running connector for the tunnel | `sudo systemctl enable --now cloudflared`; after re-creating a tunnel, reinstall the service with the new token and re-add the hostname |
| `ModuleNotFoundError: No module named 'kimi_repair'` | The service ran a copied `~/app.py` outside the repo | Run the app from `~/Finetuning-LLM` (step 4); the interim fix was a symlink `~/kimi_repair.py -> ~/Finetuning-LLM/kimi_repair.py` |
| Conversion times out on long SQL | CPU-only 7B is slow on Q8_0 | Use the Q4_K_M build (v9-q4, 1.64× faster than Q8_0 on this CPU), a GPU, serve v5 (3B), or raise the timeouts in `app.py` |

The Docker alternative (`OLLAMA_URL=http://ollama:11434 docker compose --profile ollama up -d --build`, see the [Operations Runbook](07-operations-runbook.md#-docker)) also works on Linux; this server uses native Ollama and systemd instead.

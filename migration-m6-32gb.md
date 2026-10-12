# Migration runbook: M2 MacBook Air (16 GB) → Mac mini M6 (32 GB)

The step-by-step checklist with the commands to run, from the week before delivery to the point where the new Mac
takes over. The reasons and background are in [docs/09-mac-mini-migration.md](docs/09-mac-mini-migration.md).

- **Delivery**: 2026-10-23 to 10-25. **Budget**: day 1 = steps 1–4 (~4 h), day 2 = step 5 (checks, ~2–3 h).
- **Route**: the big folders (~165 GB) are staged on the Ubuntu server (`prapon@192.168.1.49:~/m2-backup/`, mode 700)
  ahead of time; on delivery day the Mac mini pulls them over Ethernet. Secrets never go to the server: they go
  straight from the M2 to the Mac mini (step 3.1). The server copy also serves as an off-machine backup.
- **Rules**: same user name `praponb` and same paths on the new Mac; nothing is deleted on either Mac; the M2 stays
  the main machine until every check in step 5 passes.
- **Who**: 👤 = you (needs a password, `sudo`, a login or your hands), 🤖 = ask Claude Code to do it.

## Step 0 — on the M2, before delivery

### 0.1 Decide the extra folders (👤, any day)

Mark each line; the "copy" ones are added to step 3.3. Folders that hold logins or tokens (`~/.gemini`, `~/.kimi-code`,
`~/.openclaw`, `~/.config`) always go directly; the others may also be staged on the server (step 0.5).

| Folder | Size | Suggestion |
| --- | --- | --- |
| `~/Documents` | 11 GB | copy (skip if it's already in iCloud) |
| `~/Downloads` | 2.7 GB | copy (or tidy first) |
| `~/.lmstudio` | 1.9 GB | copy if you use LM Studio |
| `~/.gemini` | 1.3 GB | copy |
| `~/DataEngineerLearn` (incl. google-cloud-sdk) | 487 MB | copy |
| `~/.kimi-code` | 344 MB | copy |
| `~/.openclaw` | 76 MB | **copy** (OpenClaw / LINE bot state) |
| `~/.config` (incl. gcloud credentials) | 5 MB | copy |
| `~/Library`, `~/.npm`, `~/.vscode`, `~/.local`, `~/.antigravity-ide` | ~19 GB | leave (apps rebuild them; VS Code extensions sync) |
| Flutter (`/Applications/flutter`, `~/.pub-cache`), Android SDK | ~1 GB + | reinstall only if still used |

### 0.2 Snapshot the M2 (👤 or 🤖, on 2026-10-21 or 10-22)

```bash
cd ~/GitHub/Finetuning-LLM
open -a Ollama && sleep 5
sql2pyspark-mlx/.venv/bin/pip freeze > ~/m2-pip-freeze.txt
ollama list                          > ~/m2-ollama-list.txt
shasum -a 256 sql2pyspark*.gguf      > ~/m2-gguf.sha256
brew bundle dump --force --file ~/Brewfile
ls /Applications                     > ~/m2-apps.txt
npm ls -g --depth=0                  > ~/m2-npm-global.txt
for d in ~/GitHub/*/ ~/GitHub/Batch-to-Streaming-Converter/batch2streaming-mlx/; do
  [ -d "$d/.git" ] && echo "$d: $(git -C "$d" status --porcelain | grep -vc DS_Store) changed, $(git -C "$d" log --oneline @{u}.. 2>/dev/null | wc -l | tr -d ' ') unpushed"
done
```

Every repo should show `0 changed, 0 unpushed`. Merge any open PR first.

### 0.3 Postgres dump (👤, only if you still use the EDB install)

The EDB PostgreSQL 18 in `/Library/PostgreSQL/18` is a separate install from Homebrew's (`@17` on port 5432, `@18` on
5434 for Jobs4Dent). Homebrew's data folders are copied as files in step 3; EDB's needs a dump:

```bash
sudo grep '^port' /Library/PostgreSQL/18/data/postgresql.conf     # its port
sudo launchctl kickstart system/postgresql-18 2>/dev/null || sudo launchctl load -w /Library/LaunchDaemons/postgresql-18.plist
/Library/PostgreSQL/18/bin/pg_dumpall -U postgres -p <port> > ~/m2-edb18.sql   # asks for the postgres password
```

### 0.4 Freeze (from 2026-10-22)

No training or long chains on the M2 until step 5 is done. (v17 is not started on the M2: it is the first real run
on the M6.)

### 0.5 Stage the big folders on the server (🤖)

First pass started 2026-10-12 13:23 (log `~/m2-server-copy.log`). Run it again after the snapshot (0.2) on 10-21/22:
rsync only sends what changed. It never deletes anything, and the four `.env` files are excluded.

```bash
SRV=prapon@192.168.1.49
X=(--exclude .env --exclude sql2pyspark-hf/ --exclude '.venv*/' --exclude 'llama.cpp/build/'
   --exclude node_modules/ --exclude __pycache__/ --exclude .DS_Store)
ssh $SRV 'mkdir -p ~/m2-backup/GitHub ~/m2-backup/.ollama/models && chmod 700 ~/m2-backup'
caffeinate -i -s rsync -a $X ~/GitHub/                  $SRV:m2-backup/GitHub/                  # ~38 GB
caffeinate -i -s rsync -a ~/.ollama/models/             $SRV:m2-backup/.ollama/models/          # ~126 GB
ssh $SRV 'du -sh ~/m2-backup; df -h /'
```

The server has 295 GB free before the copy and about 130 GB after. The Hugging Face cache is not staged: it was gone
from the M2 by 2026-10-12, and the base models are downloaded again in step 4.

## Step 1 — first boot of the Mac mini (👤, ~30 min)

1. Setup assistant: create the user **`praponb`** (full name as on the M2). Sign in with your Apple ID. Update macOS.
2. System Settings → General → Sharing:
   - **Local hostname**: e.g. `Prapons-Mac-mini` (used below as `<mini>`).
   - **Remote Login** on, and **Allow full disk access for remote users** on (otherwise rsync can't write
     `~/Documents` / `~/Downloads`).
3. System Settings → Energy: **Prevent automatic sleeping when the display is off** on.
4. Connect the Mac mini to the router with an **Ethernet cable** (it pulls ~180 GB from the server: ~30–40 min on
   gigabit, hours on Wi-Fi). The M2 can stay on Wi-Fi; it only sends the small direct copies.

## Step 2 — install the tools on the Mac mini (👤, ~1 h)

In Terminal **on the Mac mini**:

```bash
xcode-select --install                     # wait for it to finish
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zprofile && eval "$(/opt/homebrew/bin/brew shellenv)"
mkdir -p ~/.ssh ~/.ollama ~/.cache/huggingface ~/GitHub && chmod 700 ~/.ssh
```

Then install by hand (download from the vendor):

- **Ollama** (ollama.com; open it once, then **quit** it from the menu bar before step 3)
- **Node.js 24 LTS** (nodejs.org macOS installer, as on the M2; not Homebrew)
- **Docker Desktop**, **Visual Studio Code**, **Google Chrome** (+ the Claude extension), **Antigravity**
- **Claude Code**: `curl -fsSL https://claude.ai/install.sh | bash` (don't log in yet; the settings come in step 3)
- The other apps from `~/m2-apps.txt` whenever you want them (Office, LINE, Raycast, Karabiner, …)

## Step 3 — copy (👤, ~1 h)

Quit Ollama on the Mac mini; Postgres stays stopped on both Macs.

### 3.1 Direct from the M2: key, settings and secrets (run **on the M2**)

These never go through the server.

```bash
NEW=praponb@<mini>.local
ssh-copy-id -i ~/.ssh/id_ed25519.pub $NEW           # password once; afterwards rsync doesn't ask
rsync -a ~/.ssh/id_ed25519 ~/.ssh/id_ed25519.pub ~/.ssh/known_hosts $NEW:.ssh/
rsync -a ~/Brewfile ~/m2-* $NEW:
rsync -a ~/.claude.json ~/.zshrc ~/.zprofile ~/.gitconfig ~/.databrickscfg $NEW:
rsync -a ~/.cloudflared/ $NEW:.cloudflared/
rsync -a --exclude cache/ --exclude shell-snapshots/ --exclude session-env/ ~/.claude/ $NEW:.claude/
tar -C ~/GitHub -cf - Finetuning-LLM/.env New_Nexus_Web/.env Agentic-AI-agents/.env OpenClaw-Line/demo/.env \
  | ssh $NEW 'tar -C ~/GitHub -xf -'                 # the 4 .env files the server copy leaves out
```

This replaces the `.zprofile` from step 2 with the M2's, which already has the `brew shellenv` line. Then **on the
Mac mini**: `chmod 600 ~/.ssh/id_ed25519 && brew bundle --file ~/Brewfile` (long; it installs Python 3.14,
openjdk@17, git, gh, postgresql@17/@18, cloudflared, …). Wait for it before 3.4.

### 3.2 Pull the big folders from the server (run **on the Mac mini**; can run while `brew bundle` runs)

```bash
SRV=prapon@192.168.1.49
ssh -o BatchMode=yes $SRV 'du -sh ~/m2-backup'      # works without a password = the key from 3.1 is in place
rsync -a --progress $SRV:m2-backup/GitHub/                  ~/GitHub/                  # ~38 GB
rsync -a --progress $SRV:m2-backup/.ollama/models/          ~/.ollama/models/          # ~126 GB
ls ~/GitHub/Finetuning-LLM/.env && echo "secrets from 3.1 still there"
```

The `.env` files from 3.1 are kept: the server copy has no `.env`, and rsync without `--delete` removes nothing.

### 3.3 Extra folders from step 0.1 (run **on the M2**; keep only the lines you marked "copy")

```bash
rsync -a --progress ~/Documents/          $NEW:Documents/
rsync -a --progress ~/Downloads/          $NEW:Downloads/
rsync -a --progress ~/.lmstudio/          $NEW:.lmstudio/
rsync -a --progress ~/.gemini/            $NEW:.gemini/
rsync -a --progress ~/DataEngineerLearn/  $NEW:DataEngineerLearn/
rsync -a --progress ~/.kimi-code/         $NEW:.kimi-code/
rsync -a --progress ~/.openclaw/          $NEW:.openclaw/
rsync -a --progress ~/.config/            $NEW:.config/
```

### 3.4 Homebrew Postgres data (run **on the M2**, after `brew bundle` finished on the Mac mini)

```bash
rsync -a /opt/homebrew/var/postgresql@17/ $NEW:/opt/homebrew/var/postgresql@17/
rsync -a /opt/homebrew/var/postgresql@18/ $NEW:/opt/homebrew/var/postgresql@18/
```

## Step 4 — rebuild on the Mac mini (👤 or 🤖, ~1 h)

Open a **new** Terminal on the Mac mini (so the copied `.zshrc` is loaded), then:

```bash
claude                      # log in once; then /exit (the memories are already there from step 3)
gh auth login               # GitHub.com, HTTPS, browser
git lfs install
npm i -g openclaw eas-cli   # as in ~/m2-npm-global.txt
python3 --version           # expect 3.14.x;  java -version  -> 17

cd ~/GitHub/Finetuning-LLM
shasum -a 256 -c ~/m2-gguf.sha256                           # both GGUFs: OK
make setup                                                  # new venv (mlx 0.32.2, mlx-lm 0.31.3, ...)
diff ~/m2-pip-freeze.txt <(sql2pyspark-mlx/.venv/bin/pip freeze) && echo "same packages"
sql2pyspark-mlx/.venv/bin/hf download mlx-community/Qwen2.5-Coder-7B-Instruct-4bit   # base of every run (4.3 GB)
sql2pyspark-mlx/.venv/bin/hf download mlx-community/Qwen3.5-9B-4bit                  # phase 5 probe only (6.0 GB)
sql2pyspark-mlx/.venv/bin/pip install cmake
cd llama.cpp && ../sql2pyspark-mlx/.venv/bin/cmake -B build -DGGML_METAL=ON -DCMAKE_BUILD_TYPE=Release \
  && ../sql2pyspark-mlx/.venv/bin/cmake --build build -j --target llama-quantize && cd ..
```

If `make setup` or MLX fails on the M6 (the pinned MLX 0.32.2 predates it), stop and ask Claude to upgrade
`mlx` / `mlx-lm` and note it in `requirements.txt`.

Other projects (when you next use each one; Claude can do these):

- `OpenClaw-Line`: `cd ~/GitHub/OpenClaw-Line && npm ci && make build test`
- `Batch-to-Streaming-Converter`: the venvs in `batch2streaming-mlx/` (`webapp/requirements.txt`) and `llama.cpp/build`
- `New_Nexus_Web`: `cd frontend && npm install`; backend venv from `backend/requirements.txt`
- `Agentic-AI-agents`: venv from `pyproject.toml` / `requirements.txt` as needed
- EDB PostgreSQL (only if dumped in 0.3): install it with the EDB installer, then
  `psql -U postgres -p <port> -f ~/m2-edb18.sql`
- Homebrew Postgres, if a project needs it: `brew services start postgresql@17` (and/or `@18`)

## Step 5 — checks before the Mac mini takes over (day 2, ~2–3 h)

Start Ollama (`open -a Ollama`) and run from `~/GitHub/Finetuning-LLM`. Or just tell Claude: **"Mac mini: run the
step 5 checks in migration-m6-32gb.md"** and it reports a pass/fail table.

| # | Check | Command | Pass |
| --- | --- | --- | --- |
| 1 | Ollama tags | `diff <(awk 'NR>1{print $1,$2}' ~/m2-ollama-list.txt \| sort) <(ollama list \| awk 'NR>1{print $1,$2}' \| sort)` | no output; `latest` = `882787d22c3c` |
| 2 | Spark + Java | `sql2pyspark-mlx/.venv/bin/python eval/evaluate.py --model sql2pyspark:v15-q4 --set eval/holdout.jsonl --sample 5` | runs, no Java errors |
| 3 | Reference eval | same without `--sample 5` | **60/62** (note s/query; M2 ~8 s) |
| 4 | App | `make app`, convert a query in the browser | works; Kimi repair works |
| 5 | MLX on the M6 | 🤖 `mx.device_info()` + 20-iteration training smoke test (copy of `schema_v15/lora_config.yaml`, own adapter path) | no errors; note s/iteration, peak memory, working-set limit |
| 6 | Export | 🤖 fuse + GGUF + quantize the smoke adapters into the scratch tag `sql2pyspark:m6-smoke-q4` | loads in Ollama; `latest` and the root GGUFs untouched |
| 7 | Server | `ssh -o BatchMode=yes prapon@192.168.1.49 'ollama list'` | lists the tags, no password |
| 8 | Claude Code | `claude` in this repo → "list all pending items" | answers from this project's memory |
| 9 | Other projects | `git status` in each repo; `ollama run batch2streaming:latest "hi"` | clean; model answers |
| 10 | OpenClaw-Line | `cd ~/GitHub/OpenClaw-Line && npm ci && make build test` | passes |
| 11 | Tunnel (👤) | see below | `batch2stream.praponb.com` / `line-bot.praponb.com` answer once their local services run |

### Check 11 — move the Cloudflare tunnel (👤, needs `sudo`)

Only one Mac may run tunnel `0644141d…`. It isn't running on the M2 right now, but make sure it stays off there:

```bash
# on the M2
sudo launchctl bootout system /Library/LaunchDaemons/com.cloudflare.cloudflared.plist 2>/dev/null
brew services stop cloudflared 2>/dev/null
sudo launchctl disable system/com.cloudflare.cloudflared       # don't start again at boot
scp /Library/LaunchDaemons/com.cloudflare.cloudflared.plist praponb@<mini>.local:/tmp/
```

```bash
# on the Mac mini (same paths: /opt/homebrew/bin/cloudflared, ~/.cloudflared/config.yml)
sudo cp /tmp/com.cloudflare.cloudflared.plist /Library/LaunchDaemons/
sudo chown root:wheel /Library/LaunchDaemons/com.cloudflare.cloudflared.plist
sudo launchctl bootstrap system /Library/LaunchDaemons/com.cloudflare.cloudflared.plist
cloudflared tunnel info 0644141d-f671-490f-a0f9-4f34fe6fe69f    # one connector, from the Mac mini
```

**All 11 pass → the Mac mini is the main machine.** If something fails, keep working on the M2 and ask Claude.

## Step 6 — after the checks (🤖)

Tell Claude: **"Mac mini checks passed: go on with phase 5, then v17."**

1. **Phase 5** (~half a day, measure only): Metal command buffers, Ollama beside training, a Qwen3.5-9B memory/speed
   probe, batch size.
2. **v17**, the first real training run on the M6, unchanged config
   (`nohup zsh eval/start_training_v17.sh > eval/start_training_v17.log 2>&1 &`; ~19.5 h on the M2, likely faster).
   Claude reports the results vs v15-q4 / v16-q4; **promotion and the server deploy are your decision** (👤
   `make promote …` and `deploy/update_model.sh` via `!`).
3. **Wrap-up**: docs updated from "M2 16 GB" to the measured M6 numbers; Claude's memory updated.
4. Keep the M2 as a fallback for a few weeks. Wiping or reusing it is your decision.
5. The server copy `~/m2-backup` (~165 GB) stays as a backup until you decide to remove it (👤
   `ssh prapon@192.168.1.49 'rm -rf ~/m2-backup'`).

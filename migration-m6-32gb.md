# Migration runbook: M2 MacBook Air (16 GB) → Mac mini M6 (32 GB)

The step-by-step checklist with the commands to run, from the week before delivery to the point where the new Mac
takes over. The reasons and background are in [docs/09-mac-mini-migration.md](docs/09-mac-mini-migration.md).

- **Delivery**: 2026-11-01 (Sunday; moved from 10-23 to 10-25 on 2026-10-22); arrived, steps 1–2 + tools done by 2026-11-08. **Budget**: day 1 = steps 1–4 (~4 h), day 2 = step 5 (checks, ~2–3 h).
- **Route** (changed 2026-11-08, owner): everything goes **directly from the M2 to the Mac mini (`192.168.1.50`)**
  over the LAN (step 3). The copy staged earlier on the Ubuntu server (`prapon@192.168.1.49:~/m2-backup/`, step 0.5)
  is not used for the copy any more; it stays only as an off-machine backup until you remove it (step 6).
- **Rules**: same user name `praponb` and same paths on the new Mac; nothing is deleted on either Mac; the M2 stays
  the main machine until every check in step 5 passes.
- **Who**: 👤 = you (needs a password, `sudo`, a login or your hands), 🤖 = ask Claude Code to do it.

## Step 0 — on the M2, before delivery

### 0.1 Decide the extra folders (👤, any day)

Decided by the owner on 2026-10-22; completed on 2026-11-08 from a fresh inventory (sizes below; full list in
`~/m2-home-inventory.txt`, made with `find ~ -mindepth 1 -maxdepth 1 ! -name GitHub -print0 | xargs -0 du -sh | sort -rh`).
The "copy" ones are in step 3.3; like everything else they go directly from the M2 to the Mac mini (some hold logins
or tokens: `~/.gemini`, `~/.kimi-code`, `~/.openclaw`, `~/.config`, `~/.hermes`, `~/.boto`,
`~/inventory-credentials-*.txt`). Outside `~/GitHub` the M2 holds ~190 GB, of which `~/.ollama/models` (125 GB) and
`~/.cache/huggingface` (6.1 GB) are copied in step 3.2.

| Folder | Size (11-08) | Suggestion | Decision |
| --- | --- | --- | --- |
| `~/Documents` | 11 GB | copy (skip if it's already in iCloud) | **copy** |
| `~/Downloads` | 2.2 GB | copy (or tidy first) | **copy** |
| `~/.hermes` (Hermes agent: tools, installs, config) | 2.4 GB | copy (`~/hermes-backup-migrate-20261013.zip` is the config backup) | **copy** (11-08) |
| `~/.lmstudio` (+ `~/.lmstudio-home-pointer`) | 2.3 GB | copy if you use LM Studio | **copy** (changed from leave, 11-08) |
| `~/b2s-macmini-transfer` (`extras/`) | 1.9 GB | copy | **copy** (11-08) |
| `~/.gemini` | 1.4 GB | copy | **copy** |
| `~/DataEngineerLearn` (incl. google-cloud-sdk) | 723 MB | copy | **copy** |
| `~/.kimi-code` | 344 MB | copy | **copy** |
| `~/.openclaw` | 75 MB | copy (OpenClaw / LINE bot state) | **copy** (backup only, see check 11) |
| `~/.config` (incl. gcloud credentials) | 84 MB | copy | **copy** |
| Small items: `~/inventory-backups/`, `~/inventory-credentials-20260828.txt`, `~/SKILL.md`, `~/hermes-backup-migrate-20261013.zip`, `~/2026-10-10T09-35-52.795+07-00-openclaw-backup.tar.gz`, `~/.boto`, `~/.ipython/`, `~/.streamlit/` | ~11 MB | copy | **copy** (11-08) |
| `~/Library` (27 GB, of which 15 GB is Docker's disk image in `Containers/com.docker.docker` and 5.8 GB `Caches`), `~/.cache/uv`, `~/.npm`, `~/.vscode`, `~/.local`, `~/.antigravity-ide`, `~/.antigravity`, `~/.gem`, `~/.cocoapods` | ~35 GB | leave (apps rebuild them; VS Code extensions sync; Docker images are rebuilt) | **leave** |
| Flutter, Android SDK | — | gone from the M2 (11-08: no `/Applications/flutter`, `~/Library/Android`, `~/.android`; only `~/.pub-cache` 862 MB and `~/.dartServer` 188 MB are left, and `~/.zprofile` / `~/.zshrc` still put them on `PATH`) | **leave** (not reinstalled) |

### 0.2 Snapshot the M2 (👤 or 🤖, on 2026-10-30 or 10-31)

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

### 0.3 Postgres

Only Homebrew's Postgres moves (`@17` on port 5432, `@18` on 5434 for Jobs4Dent): its data folders are copied as files
in step 3.4. The EDB PostgreSQL 18 in `/Library/PostgreSQL/18` is not used (owner, 2026-10-12): not copied, not
reinstalled.

### 0.4 Freeze (from 2026-10-31)

No training or long chains on the M2 until step 5 is done. (v17 is not started on the M2: it is the first real run
on the M6.)

### 0.5 Stage the big folders on the server (🤖) — not used any more

**Skipped from 2026-11-08**: the copy now goes directly from the M2 to the Mac mini (step 3.2), so the server copy is
not re-synced (it misses `metalsql`, created 11-05). Kept below for the record.

First pass done 2026-10-12 (13:23–16:42, log `~/m2-server-copy.log`; checked: Ollama identical, GitHub identical except
files deleted on the M2 afterwards). Run it again after the snapshot (0.2) on 10-30/31: rsync only sends what changed,
and **`--delete`** removes from the server copy whatever was deleted on the M2 since, so it doesn't come back on the Mac
mini. `--delete` only acts inside `~/m2-backup/GitHub` and `~/m2-backup/.ollama/models` on the server, never on the M2,
and excluded files (`.env`, venvs, …) are left alone. The five `.env` files are never sent.

```bash
SRV=prapon@192.168.1.49
SSH=(-e "ssh -o ServerAliveInterval=15 -o ServerAliveCountMax=8")   # survives short Wi-Fi drops
X=(--exclude .env --exclude sql2pyspark-hf/ --exclude '.venv*/' --exclude 'llama.cpp/build/'
   --exclude node_modules/ --exclude __pycache__/ --exclude .DS_Store)
ssh $SRV 'mkdir -p ~/m2-backup/GitHub ~/m2-backup/.ollama/models && chmod 700 ~/m2-backup'
rsync -a -n -v --delete $SSH $X ~/GitHub/ $SRV:m2-backup/GitHub/ | grep '^deleting'   # dry run: review first
caffeinate -i -s rsync -a --delete $SSH $X ~/GitHub/        $SRV:m2-backup/GitHub/          # ~38 GB
caffeinate -i -s rsync -a --delete $SSH ~/.ollama/models/   $SRV:m2-backup/.ollama/models/  # ~126 GB
ssh $SRV 'df -h /'
```

Keep the trailing `/` on every source and target path: without it rsync copies into a subfolder, and `--delete` would
then compare the wrong folders. If a run stops ("Broken pipe"), run the same line again; finished files are skipped.

The server had 295 GB free before the first pass, 133 GB after it and 116 GB on 2026-10-22. The Hugging Face cache is not staged: it was gone
from the M2 by 2026-10-12, and the base models are downloaded again in step 4.

## Step 1 — first boot of the Mac mini (👤, ~30 min)

1. Setup assistant: create the user **`praponb`** (full name as on the M2). Sign in with your Apple ID. Update macOS.
2. System Settings → General → Sharing:
   - **Local hostname**: e.g. `Prapons-Mac-mini` (the commands below use the LAN address `192.168.1.50`).
   - **Remote Login** on, and **Allow full disk access for remote users** on (otherwise rsync can't write
     `~/Documents` / `~/Downloads`).
3. System Settings → Energy: **Prevent automatic sleeping when the display is off** on.
4. Connect the Mac mini to the router with an **Ethernet cable**; its LAN address is **`192.168.1.50`**. It receives
   ~190 GB from the M2 (step 3), so the M2's link is the limit: ~1 h on Wi-Fi at the ~50 MB/s measured on
   2026-10-12, less with a USB-C Ethernet adapter on the M2.

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

Quit Ollama on both Macs (the M2 sends `~/.ollama/models` in 3.2); Postgres stays stopped on both Macs.

### 3.1 Key and settings (run **on the M2**, first)

```bash
NEW=praponb@192.168.1.50
ssh-copy-id -i ~/.ssh/id_ed25519.pub $NEW           # password once; afterwards rsync doesn't ask
rsync -a ~/.ssh/id_ed25519 ~/.ssh/id_ed25519.pub ~/.ssh/known_hosts $NEW:.ssh/
rsync -a ~/Brewfile ~/m2-* $NEW:
rsync -a ~/.claude.json ~/.zshrc ~/.zprofile ~/.gitconfig ~/.databrickscfg $NEW:
rsync -a ~/.cloudflared/ $NEW:.cloudflared/
rsync -a --exclude cache/ --exclude shell-snapshots/ --exclude session-env/ ~/.claude/ $NEW:.claude/
```

This replaces the `.zprofile` from step 2 with the M2's, which already has the `brew shellenv` line. Then **on the
Mac mini**: `chmod 600 ~/.ssh/id_ed25519 && brew bundle --file ~/Brewfile` (long; it installs Python 3.14,
openjdk@17, git, gh, postgresql@17/@18, cloudflared, …). Wait for it before 3.4.

### 3.2 The big folders, directly from the M2 (run **on the M2**; can run while `brew bundle` runs)

```bash
NEW=praponb@192.168.1.50
SSH=(-e "ssh -o ServerAliveInterval=15 -o ServerAliveCountMax=8")   # survives short Wi-Fi drops
X=(--exclude sql2pyspark-hf/ --exclude '.venv*/' --exclude 'llama.cpp/build/'
   --exclude node_modules/ --exclude __pycache__/ --exclude .DS_Store)
ssh -o BatchMode=yes $NEW 'df -h /'                  # works without a password = the key from 3.1 is in place
caffeinate -i -s rsync -a --progress $SSH $X ~/GitHub/                 $NEW:GitHub/                  # ~38 GB
caffeinate -i -s rsync -a --progress $SSH    ~/.ollama/models/         $NEW:.ollama/models/          # ~125 GB
caffeinate -i -s rsync -a --progress $SSH    ~/.cache/huggingface/hub/ $NEW:.cache/huggingface/hub/  # 6.1 GB
ssh $NEW 'cd ~/GitHub && ls Finetuning-LLM/.env New_Nexus_Web/.env Agentic-AI-agents/.env OpenClaw-Line/demo/.env \
  Hadoop-to-Lakehouse/.env metalsql/.env'           # the 6 secret files: they travel with ~/GitHub now
```

Keep the trailing `/` on every path. If a run stops ("Broken pipe"), run the same line again; finished files are
skipped. The `.env` files are no longer excluded: they go straight to the Mac mini and never touch the server. The
Hugging Face cache holds the base model re-downloaded on 2026-10-22, so step 4's `hf download` finds it cached.

### 3.3 Extra folders from step 0.1 (run **on the M2**; keep only the lines you marked "copy")

```bash
rsync -a --progress ~/Documents/          $NEW:Documents/
rsync -a --progress ~/Downloads/          $NEW:Downloads/
rsync -a --progress ~/.gemini/            $NEW:.gemini/
rsync -a --progress ~/DataEngineerLearn/  $NEW:DataEngineerLearn/
rsync -a --progress ~/.kimi-code/         $NEW:.kimi-code/
rsync -a --progress ~/.openclaw/          $NEW:.openclaw/
rsync -a --progress ~/.config/            $NEW:.config/
rsync -a --progress ~/.hermes/            $NEW:.hermes/
rsync -a --progress ~/b2s-macmini-transfer/ $NEW:b2s-macmini-transfer/
rsync -a --progress ~/.lmstudio/          $NEW:.lmstudio/
rsync -a --progress ~/.lmstudio-home-pointer $NEW:
rsync -a --progress ~/inventory-backups ~/.ipython ~/.streamlit $NEW:     # no trailing slash: copies the folders
rsync -a --progress ~/inventory-credentials-20260828.txt ~/SKILL.md ~/.boto \
  ~/hermes-backup-migrate-20261013.zip ~/2026-10-10T09-35-52.795+07-00-openclaw-backup.tar.gz $NEW:
```

Together ~22 GB (Documents 11 GB): roughly 8–10 min from the M2 over Wi-Fi at the ~50 MB/s measured on 2026-10-12.

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
`mlx` / `mlx-lm` and note it in `requirements.txt`. Tested on the M2 on 2026-10-22 (v15 config, short smoke runs):
**mlx 0.32.3 + mlx-lm 0.32.2** trains with the same losses as the pin, so upgrade to that pair. **Never mlx 0.32.4**:
the first optimizer update turns every LoRA weight into NaN (with either mlx-lm, with or without `grad_checkpoint`
and the small Metal buffers). Fuse and inference are not affected (the fused v15 is byte-identical).

Other projects (when you next use each one; Claude can do these):

- `OpenClaw-Line`: `cd ~/GitHub/OpenClaw-Line && npm ci && make build test`
- `Batch-to-Streaming-Converter`: the venvs in `batch2streaming-mlx/` (`webapp/requirements.txt`) and `llama.cpp/build`
- `New_Nexus_Web`: `cd frontend && npm install`; backend venv from `backend/requirements.txt`
- `Agentic-AI-agents`: venv from `pyproject.toml` / `requirements.txt` as needed
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
| 11 | Tunnels (👤) | see below | the M2 and the Mac mini run no connector for `0644141d…`; `batch2stream.praponb.com` answers |

### Check 11 — Cloudflare tunnels (👤, needs `sudo`)

Nothing moves to the Mac mini. Since 2026-10-13 tunnel `0644141d…` (`batch2stream.praponb.com`) runs on the Ubuntu
server (systemd `cloudflared-batch2stream`, app `batch2stream-web` on 127.0.0.1:8100). Only one machine may run a
connector for it (a second one splits the requests), so it stays off on both Macs. On the M2 it isn't running, but its
LaunchDaemon is still installed and enabled (`RunAtLoad`), so switch it off for good:

```bash
# on the M2
sudo launchctl bootout system /Library/LaunchDaemons/com.cloudflare.cloudflared.plist 2>/dev/null
brew services stop cloudflared 2>/dev/null
sudo launchctl disable system/com.cloudflare.cloudflared       # don't start again at boot
```

```bash
# on the Mac mini: brew bundle only installs the binary; don't install the LaunchDaemon or start the service
brew services list | grep cloudflared                          # "none" or "stopped"
cloudflared tunnel info 0644141d-f671-490f-a0f9-4f34fe6fe69f   # one connector: linux_amd64 (the server)
curl -sI https://batch2stream.praponb.com | head -1            # 302 (Cloudflare Access login)
```

`line-bot.praponb.com` is a leftover and stays at 404 (owner, 2026-10-22). The LINE channel's webhook already goes to
the server: `https://openclaw-feedback.praponb.com/line/webhook` (the OpenClaw-Line worker, tunnel `582a2422…`). The
M2's personal OpenClaw gateway (`~/.openclaw`, LaunchAgent `ai.openclaw.gateway`, :18789) used the same channel and is
not moved: a channel has only one webhook URL. `~/.openclaw` is still copied (step 0.1) as a backup; don't start a
gateway from it on the Mac mini.

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

## Step 7 — retire the M2 (owner's decision, 2026-11-12)

Checked from the Mac mini on 2026-11-12: the M2 serves nothing any more. Each tunnel has one connector:
`sql2pyspark` and `metalsql` on the Mac mini (darwin_arm64, since 11-08), `0644141d…` (batch2stream) on the Ubuntu
server. The Mac mini has run everything since 2026-11-08 (training, both sites, Claude Code with this project's memory).

**Keep**: the server copy `~/m2-backup` (~165 GB, the M2 snapshot of 2026-10-22) stays as the last copy of the M2
until you remove it (👤 `ssh prapon@192.168.1.49 'rm -rf ~/m2-backup'`). The Mac mini's SSH key `~/.ssh/id_ed25519` is
**the same key** the M2 used (copied in step 3.1): don't revoke it on GitHub or remove it from the server's
`authorized_keys`, or the Mac mini loses access too. Erasing the M2 removes its copy.

### 7.1 Last look for anything not yet on the Mac mini (👤, on the M2, ~15 min)

```bash
NEW=praponb@192.168.1.50
# git: uncommitted or unpushed work in any repo (expect: none, or only the known untracked files)
for d in ~/GitHub/*/.git; do r=${d%/.git}; s=$(git -C $r status --porcelain | head -3); \
  u=$(git -C $r log --oneline @{u}.. 2>/dev/null | head -3); [[ -n $s$u ]] && echo "== $r" && echo "$s" && echo "$u"; done
# files newer on the M2 than on the Mac mini (dry run, nothing is sent; -u skips files the Mac mini changed since)
rsync -anu --itemize-changes --exclude sql2pyspark-hf/ --exclude '.venv*/' --exclude 'llama.cpp/build/' \
  --exclude node_modules/ --exclude __pycache__/ --exclude .DS_Store ~/GitHub/ $NEW:GitHub/ | grep -v '^\.' | head -50
for f in Documents Downloads Desktop Pictures Movies Music; do echo "== $f"; \
  find ~/$f -type f -newermt 2026-11-08 2>/dev/null | head -5; done
```

Anything listed that you still need: copy it with the step 3.2 / 3.3 lines (without `-n`). Also check by hand what
lives only in apps on the M2: Safari / Chrome bookmarks and passwords (if not synced), Notes and Photos (if not in
iCloud), LM Studio / Docker images (rebuildable), other Keychain items.

### 7.2 Switch off the M2's services and sign out (👤, on the M2)

```bash
# Cloudflare: the LaunchDaemon of tunnel 0644141d was disabled in check 11; remove it for good
sudo launchctl bootout system/com.cloudflare.cloudflared 2>/dev/null
sudo rm -f /Library/LaunchDaemons/com.cloudflare.cloudflared.plist
brew services stop --all
pkill -x cloudflared; killall Ollama 2>/dev/null; pkill -x ollama
launchctl bootout gui/$(id -u)/ai.openclaw.gateway 2>/dev/null     # personal OpenClaw gateway (not moved)
gh auth logout                                                      # GitHub CLI token of this Mac
```

Then in the apps: sign out of Claude Code (`/logout`) and Docker Desktop, deauthorise the M2 in Music / TV (Account →
Authorisations), and unpair Bluetooth devices you'll use with the Mac mini.

### 7.3 Erase (👤, irreversible)

1. System Settings → [your name] → **Sign Out** of the Apple Account (turns off Find My and Activation Lock; keep a
   copy of the data in iCloud when asked).
2. System Settings → General → Transfer or Reset → **Erase All Content and Settings**.

The M2 is then ready to sell, give away or reuse. Tell Claude when it's done, so the docs and memory say so.

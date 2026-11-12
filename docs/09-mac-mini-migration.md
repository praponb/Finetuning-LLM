# 09 - Migration to the Mac mini M6 (32 GB)

> **Status (2026-11-12): done.** The Mac mini M6 (32 GB, 2 TB, 192.168.1.50) arrived on 2026-11-08 (the plan said
> 11-01); the data was copied straight from the M2 ([migration-m6-32gb.md](../migration-m6-32gb.md) has the commands
> as run), all 11 phase 4 checks passed on 11-08, phase 5 was measured the same afternoon and v17 ran that evening.
> sql2pyspark.praponb.com and metalsql.praponb.com moved to the Mac mini too (LaunchDaemons;
> [deploy/macmini](../deploy/macmini/README.md)); the Ubuntu server copy of sql2pyspark is kept only for rollback. The
> rest of this page is the plan as approved, with the results added below.

**Goal**: move training, export, evaluation, the local app and the server deploys — and the owner's other projects in
`~/GitHub` — from the base **M2 MacBook (16 GB)** to a **Mac mini M6 (32 GB)** without losing anything, then prove the
new Mac gives the same results before trusting it. The Ubuntu server (192.168.1.49) and sql2pyspark.praponb.com don't
change.

## Decisions (2026-10-10)

| Question | Decision |
| --- | --- |
| Storage | 2 TB: copy everything, including all Ollama models (no pruning) |
| Transfer method | Clean setup + `rsync` (not Migration Assistant): venvs and builds are recreated for the M6 |
| Other projects in `~/GitHub` | Move them in the same session ([Other projects](#other-projects)) |
| Re-measuring the 16 GB settings (phase 5) | Right after phase 4 passes, in the order listed there |
| v17 (prepared 2026-10-10, commit 772ad91, never started on the M2) | First real training run on the M6, after phase 5, with the unchanged config (decided 2026-10-12) |

## Principles

- The M2 stays untouched and usable until the new Mac passes every check in phase 4. Nothing is deleted on either machine.
- Keep the **same macOS user name (`praponb`) and the same paths (`~/GitHub/...`)**. 17 tracked chain scripts
  (`eval/start_training_v*.sh`, `post_training_v*.sh`, `qwen35_pilot/chain.sh`, …) and Claude Code's memory folders
  (`~/.claude/projects/-Users-praponb-GitHub-<project>/`) depend on them.
- First reproduce, then change. The first training run on the new Mac uses an unchanged config, so results are
  comparable. Tuning for 32 GB comes after that.

## What has to move (measured on the M2, 2026-10-10)

| Item | Size | How | Notes |
| --- | --- | --- | --- |
| This repo `~/GitHub/Finetuning-LLM` (tracked + `.git`) | ~3.4 GB `.git` | copy | `git clone` would miss everything in the next row, so copy the folder |
| Ignored files in this repo: `sql2pyspark.gguf` (v15, 7.5 GB), `sql2pyspark-q4.gguf` (v15-q4, 4.4 GB), `raw_*` folders, LoRA checkpoints, `qwen35_pilot/{gguf,data_long,data_mix2600}` (≈5.5 GB), `.env` (Kimi key) | ≈27 GB | copy with the repo | The GGUFs are what `deploy/update_model.sh` sends to the server |
| `sql2pyspark-hf/` | 14 GB | **don't copy** | Rebuildable export leftover (`make clean` target); stays on the M2 |
| `sql2pyspark-mlx/.venv/` | 2.1 GB | **recreate** (`make setup`) | venvs hold absolute paths and compiled wheels |
| `llama.cpp/` (checkout, 627 MB) | | copy, then **rebuild** `build/` | `llama-quantize` was built with the venv's pip `cmake` |
| Ollama models `~/.ollama/models` | **125 GB** | copy all | All 22 `sql2pyspark:*` tags (incl. the kept `q35-pilot-q4`, `base-q35-q4`, `base-coder7b-q4`) + `batch2streaming:latest` |
| Hugging Face cache `~/.cache/huggingface/hub` | 15 GB | copy | Qwen2.5-Coder-7B-4bit (the base for every run), Qwen3.5-9B-4bit, Qwen2.5-Coder-3B |
| Other projects in `~/GitHub` | ≈14 GB | copy | See [Other projects](#other-projects) |
| Claude Code `~/.claude/` | 101 MB | copy | settings + the project memories of every project |
| SSH key `~/.ssh/id_ed25519` (+ `.pub`) | | copy | The server trusts this key; the deploy uses `scp` / `ssh` from the Mac |
| Homebrew Postgres data (`/opt/homebrew/var/postgresql@17`, `@18`) | 170 MB | copy while stopped | Both services are stopped on the M2; same major versions on the new Mac |
| EDB PostgreSQL 18 (`/Library/PostgreSQL/18`, LaunchDaemon `postgresql-18`, pgAdmin 4) | | `pg_dumpall` in phase 0, restore on the new Mac | A separate install beside Homebrew's; its data needs `sudo`, so the owner runs the dump and the restore |
| Cloudflare tunnel `~/.cloudflared/` (`config.yml` + credentials) | | copy; cut over in phase 4 | Tunnel `0644141d…` serves `batch2stream.praponb.com` (→ :8100) and `line-bot.praponb.com` (→ :18789) **from this Mac** (LaunchDaemon `com.cloudflare.cloudflared`, brew service `sh.brew.cloudflared`). Only one Mac may run it. sql2pyspark.praponb.com is on the server and doesn't change |
| Dotfiles `~/.zshrc`, `~/.zprofile`, `~/.gitconfig`, `~/.databrickscfg` | | copy | `PATH` also names `~/.kimi-code/bin`, the Android SDK, `/Applications/flutter`, `~/DataEngineerLearn/google-cloud-sdk`, `~/.lmstudio/bin`: those come from the phase 0 inventory |
| Node.js 24.16 (nodejs.org package in `/usr/local`, not Homebrew) + global npm `openclaw`, `eas-cli` | | reinstall | `npm ls -g --depth=0` in phase 0 |
| **Total to copy** | **≈185 GB** | | ~9% of the 2 TB |

Docker Desktop's VM (9.5 GB) is not copied: reinstall Docker Desktop (OpenClaw-Line's demo uses Docker compose) and
rebuild the images when needed (this repo's `tools` / `ollama` compose profiles; the server doesn't use Docker). The Android SDK / AVDs (`~/Library/Android`,
`~/.android`) are not part of this plan; reinstall them through Android Studio if wanted.

## Other projects

| Folder | Size | Git | Move | Rebuild on the new Mac |
| --- | --- | --- | --- | --- |
| `Batch-to-Streaming-Converter/` | 15 GB | **the top folder has no git**; `batch2streaming-mlx/` is a repo (praponb/Batch-to-Streaming-Converter), clean, pushed | copy the whole folder (the top folder exists only on the M2) except the venvs | `.venv`, `batch2streaming-mlx/.venv`, `.venv-gguf`, `.venv-validate` (3 GB together; `webapp/requirements.txt`), `llama.cpp/build`. Keeps `batch2streaming-3b.gguf`, `batch2streaming-3b-fused/`, adapters v5 / v6 |
| `OpenClaw-Line/` (praponb/OpenClaw-Line, new 2026-10-10) | 802 MB (793 MB `node_modules`) | clean, pushed | copy except `node_modules`; has `demo/.env` (copied) | `npm ci` (Node 24.16, `.nvmrc`), `make build test`; the demo needs Docker Desktop and the tunnel |
| `New_Nexus_Web/` | 1.3 GB | clean, pushed | copy except `frontend/node_modules` | `npm install` in `frontend/`, venv from `backend/requirements.txt`; has `.env` (copied) |
| `Agentic-AI-agents/` (praponb/Digital-Twin) | 209 MB | clean, pushed | copy | venv from `pyproject.toml` / per-folder `requirements.txt` as needed; has `.env` (copied) |
| `Senior-Data-Engineer-Test/` | 5.5 MB | clean except an untracked `.DS_Store` | copy | `Task1/requirements.txt` if used |
| `antigravity-sdk-python/`, `ttb-branch-finder/` | 5 MB | clean, pushed | copy | as needed |

Claude Code memory folders for `Jobs4Dent-ClaudeCode` and `Nexus-ClaudeCode` exist in `~/.claude/projects/` without a
matching folder in `~/GitHub`; they come along with `~/.claude/` and stay unused.

## Phase 0 — on the M2, before 2026-10-23

1. Merge every open PR (PR #30: done 2026-10-10), so `main` and `b_pbv_main` are both pushed; `git status` clean in every repo.
2. Inventory of everything outside `~/GitHub` (any day before): `du -sh ~/* ~/.[!.]*`. The owner marks each folder
   (e.g. `~/DataEngineerLearn`, `~/.kimi-code`, `~/.lmstudio` 1.9 GB, Flutter, the Android SDK) copy / reinstall / leave;
   the "copy" ones are added to the phase 2 rsync. **Done** (2026-10-22, completed 2026-11-08): the decisions and the
   rsync lines are in [`migration-m6-32gb.md`](../migration-m6-32gb.md) steps 0.1 and 3.3.
3. Record the reference environment for later comparison (on 2026-10-21 or 10-22, so it is current):

   ```bash
   cd ~/GitHub/Finetuning-LLM
   sql2pyspark-mlx/.venv/bin/pip freeze > ~/m2-pip-freeze.txt
   ollama list > ~/m2-ollama-list.txt
   shasum -a 256 sql2pyspark*.gguf > ~/m2-gguf.sha256
   brew bundle dump --file ~/Brewfile          # formulae + casks, reinstalled in phase 1
   ls /Applications > ~/m2-apps.txt            # apps not from Homebrew (Office, LINE, LM Studio, Raycast, …): reinstall by hand
   npm ls -g --depth=0 > ~/m2-npm-global.txt
   ```

   The owner dumps every Postgres instance in use (EDB needs the `postgres` password), e.g.
   `/Library/PostgreSQL/18/bin/pg_dumpall -U postgres > ~/m2-edb18.sql`.
4. No training or long chain runs from 2026-10-22 until the copy is done.

## Phase 1 — set up the Mac mini (day 1, ~1–2 h)

1. First boot: create the user **`praponb`** (same short name). Update macOS. Turn on **Remote Login** (System
   Settings → General → Sharing) for the copy in phase 2.
2. Energy: System Settings → Energy → "Prevent automatic sleeping when the display is off" on. The Mac mini has no
   lid, so the M2's lid-sleep problem during training (v14) goes away; the chains still run `caffeinate -s -i`.
3. Tools:
   - Xcode Command Line Tools: `xcode-select --install`
   - Homebrew, then `brew bundle --file ~/Brewfile` (after copying `~/Brewfile` over; it includes `python@3.14`,
     `openjdk@17`, `git`, `git-lfs`, `gh`, `node`, `postgresql@17` / `@18`, `cloudflared`, `databricks`, …); put
     `openjdk@17` on `PATH` the same way as on the M2 (`/opt/homebrew/opt/openjdk@17/bin`); Spark needs Java 17
   - Apps: Ollama (the latest; the M2 has 0.35.1, the server 0.34.4: the GGUF blobs work on both), Visual Studio Code,
     Google Chrome (+ the Claude extension), Antigravity, Docker Desktop, Node.js 24 (nodejs.org package, as on the
     M2), then `npm i -g openclaw eas-cli`; the rest of `~/m2-apps.txt` as wanted
   - Claude Code (native install), then log in
4. Accounts: `gh auth login` (https), `git config --global user.name "Prapon (Aek) Bavonparadon"` and
   `user.email 119161666+praponb@users.noreply.github.com`, `git lfs install`.

## Phase 2 — copy the data (day 1, ~1 h over a cable)

Connect the two Macs with a Thunderbolt / USB-C cable (Thunderbolt Bridge) or wired Ethernet: ≈185 GB takes roughly
30–60 min on a cable, several hours on Wi-Fi. Quit Ollama on both machines and keep Postgres stopped, then run on the
**M2**:

```bash
NEW=praponb@<mac-mini>.local
rsync -a --progress \
  --exclude sql2pyspark-hf/ --exclude 'sql2pyspark-mlx/.venv/' --exclude 'llama.cpp/build/' \
  --exclude '.venv*/' --exclude node_modules/ --exclude __pycache__/ \
  ~/GitHub/ $NEW:GitHub/
rsync -a --progress ~/.ollama/models/ $NEW:.ollama/models/
rsync -a --progress ~/.cache/huggingface/hub/ $NEW:.cache/huggingface/hub/
rsync -a --progress --exclude cache/ --exclude shell-snapshots/ --exclude session-env/ ~/.claude/ $NEW:.claude/
rsync -a ~/.claude.json ~/Brewfile ~/m2-* $NEW:
rsync -a ~/.zshrc ~/.zprofile ~/.gitconfig ~/.databrickscfg $NEW:
rsync -a ~/.cloudflared/ $NEW:.cloudflared/
scp ~/.ssh/id_ed25519 ~/.ssh/id_ed25519.pub $NEW:.ssh/
rsync -a /opt/homebrew/var/postgresql@17 /opt/homebrew/var/postgresql@18 $NEW:/opt/homebrew/var/   # after brew bundle on the new Mac
```

`--exclude '.venv*/'`, `--exclude node_modules/` and `--exclude 'llama.cpp/build/'` apply in every project (venvs,
`node_modules` and builds are recreated). Add the folders marked "copy" in the phase 0 inventory the same way.
Then on the Mac mini: `chmod 600 ~/.ssh/id_ed25519`, and compare `shasum -a 256 ~/GitHub/Finetuning-LLM/sql2pyspark*.gguf`
with `~/m2-gguf.sha256`.

## Phase 3 — rebuild the environments (day 1, ~1 h)

This project:

```bash
cd ~/GitHub/Finetuning-LLM
make setup                                   # new venv from requirements.txt (pinned: mlx 0.32.2, mlx-lm 0.31.3, …)
sql2pyspark-mlx/.venv/bin/pip install cmake  # the M2 built llama.cpp with the venv's cmake
cd llama.cpp && cmake -B build -DGGML_METAL=ON -DCMAKE_BUILD_TYPE=Release && cmake --build build -j --target llama-quantize
```

Compare `pip freeze` with `~/m2-pip-freeze.txt`. **Risk:** the pinned MLX 0.32.2 predates the M6. If it fails or is
slow on the M6 (check in phase 4), upgrade `mlx` / `mlx-lm` to the first versions that support it, update
`requirements.txt`, and note that later runs used a newer MLX.

Other projects: recreate the venvs / `node_modules` listed in [Other projects](#other-projects) when each project is
next used (Batch-to-Streaming-Converter: the three venvs in `batch2streaming-mlx/` and `llama.cpp/build` first, since
it shares the MLX toolchain). OpenClaw-Line: `npm ci`, then `make build test`. `brew services start postgresql@17`
(or `@18`) only if a project needs it; EDB PostgreSQL 18: install it with the EDB installer and restore
`~/m2-edb18.sql` with `psql -U postgres -f` (owner, needs the password).

## Phase 4 — verify before trusting the new Mac (day 1–2, ~2–3 h)

Each check has a pass bar. Until all of them pass, the M2 stays the main machine.

| # | Check | Command / how | Pass bar |
| --- | --- | --- | --- |
| 1 | Ollama has every tag | `ollama list` vs `~/m2-ollama-list.txt` | same tags, same IDs (`latest` = `882787d22c3c`) |
| 2 | Spark + Java | `eval/evaluate.py --model sql2pyspark:v15-q4 --set eval/holdout.jsonl --sample 5` | runs, no Java errors |
| 3 | Reference eval | `eval/evaluate.py --model sql2pyspark:v15-q4 --set eval/holdout.jsonl` | **60/62**, as on the M2 (also note s/query: M2 ~8 s) |
| 4 | App | `make app`, convert a query in the browser | works; Kimi repair works with `.env` |
| 5 | MLX works on the M6 | `python -c "import mlx.core as mx; print(mx.device_info())"` + a 20-iteration training smoke test (copy of `schema_v15/lora_config.yaml`, `iters: 20`, separate adapter path) | no errors; note `max_recommended_working_set_size` (M2: 12.7 GB), s/iteration and peak memory (M2 v15: ~6 s / iteration, ~10 GB) |
| 6 | Export | fuse + GGUF + `llama-quantize` of the smoke-test adapters into a **scratch** tag (`sql2pyspark:m6-smoke-q4`) | GGUF loads in Ollama; never touch `latest` or the root GGUFs here |
| 7 | Server access | `ssh -o BatchMode=yes prapon@192.168.1.49 'ollama list'` | lists the server tags (`latest` = v15-q4) without a password |
| 8 | Claude Code | start `claude` in the repo, ask for pending items | it reads this project's memory |
| 9 | Other projects | `git status` in each repo; `ollama run batch2streaming:latest` answers | clean (as on the M2); model loads |
| 10 | OpenClaw-Line | `npm ci && make build test` | passes |
| 11 | Tunnel cutover (owner runs the `sudo` steps) | stop cloudflared on the M2 (`sudo launchctl bootout system/com.cloudflare.cloudflared`, `brew services stop cloudflared`), then `sudo cloudflared service install` on the new Mac | `cloudflared tunnel info 0644141d…` shows one connector (the new Mac); both hostnames answer once their local services run |

The deploy script (`deploy/update_model.sh`) only needs check 7 plus the root GGUFs; no test deploy is needed until
the next model is promoted.

## Phase 5 — re-measure the 16 GB settings (right after phase 4, ~half a day)

Measured in this order, each on its own; the results go into the docs in phase 6:

1. **Metal command buffers**: `train_resumable.py` sets `MLX_MAX_OPS_PER_BUFFER=2` / `MLX_MAX_MB_PER_BUFFER=40` (~9%
   slower) against the macOS GPU watchdog. Run 50 iterations of the v15 config with the MLX defaults
   (`MLX_MAX_OPS_PER_BUFFER` / `MLX_MAX_MB_PER_BUFFER` unset in a copy of the launcher, or exported to the defaults);
   if there is no `Impacting Interactivity` abort and it is faster, make the small buffers opt-in.
2. **Ollama beside training**: during a 50-iteration run, score 5 hold-out queries with Ollama. If there's no Metal OOM
   and memory stays below the working-set limit, evals may overlap training from now on.
3. **Qwen3.5-9B probe**: the pilot failed on memory (12.62 GB even with patches, bar 12.5) and speed (~14 h per full
   run). A 20-iteration memory and speed probe with stock mlx-lm (`qwen35_pilot/memtest.yaml`, without the
   `gd_chunked` / `chunked_loss` patches, then with them) shows whether a full run is realistic now. Whether to
   do a full run stays the owner's decision.
4. **Batch size** (measure only): whether batch 2–4 without gradient accumulation fits and how fast it is. It changes
   training dynamics, so it's used only on a future new run, never on a rerun of an existing version.

**Results (2026-11-08 15:44 → 16:03, v15 config copies):**

| # | Measurement | Result | Consequence |
| --- | --- | --- | --- |
| 1 | Metal command buffers | MLX defaults 154.6 tok/s, peak 8.64 GB; small 2 / 40 buffers 159.2 tok/s, 8.42 GB; no watchdog abort with either | small buffers are not slower on the M6: `train_resumable.py` unchanged |
| 2 | Ollama beside training | no OOM (training 8.64 GB + v15-q4 6.2 GB); training −20–30% (105–125 tok/s), 5 hold-out queries 5/5 at ~9 s each (2.7 s alone) | overlap allowed, but it slows both; the public site's Ollama runs during training |
| 3 | Qwen3.5-9B probe (64 longest samples, max_seq 3200, 8 layers) | stock mlx-lm 0.31.3: Metal OOM even on 32 GB; with the patches: peak 14.6 GB, 95 tok/s (M2 ~30) | a full run is ~3× faster than on the M2; the owner then ran 8 Qwen3.5 rounds (stopped 2026-11-12, see [qwen35_pilot](../sql2pyspark-mlx/qwen35_pilot/README.md)) |
| 4 | Batch size without accumulation | batch 2: 90–103 tok/s, peak 13.9 GB; batch 4: 73 tok/s, 18.9 GB | slower than batch 1 × 4 accumulation (155 tok/s): kept |

Phase 4 reference numbers: hold-out 60/62 with v15-q4 (as on the M2) at 2.72 s / query (M2 ~6–8 s); MLX working set
26.8 GB, smoke test ~1.8 s / iteration, peak 8.3 GB, same losses as the M2. MLX stayed at mlx 0.32.2 / mlx-lm 0.31.3.

## v17 — the first training run on the M6 (after phase 5)

v17 (v15 + its full mix + 4,000 complex pairs, 4,800 iterations; [complex_v17](../sql2pyspark-mlx/complex_v17/))
was prepared on the M2 but never started there. It runs on the new Mac with the **unchanged** config and chain, so its
scores compare with v15 / v16:

```bash
cd ~/GitHub/Finetuning-LLM && nohup zsh eval/start_training_v17.sh > eval/start_training_v17.log 2>&1 &
```

The M2 estimate was ~19.5 h (training ~10.3 h); the phase 4 smoke test gives the M6 time per iteration. If phase 5
showed that Ollama can run beside training, that is used only from v18 on. Promotion and the server deploy stay the
owner's decision.

**Result**: done 2026-11-08 — training 16:03 → 19:05 (**~3 h** vs. ~10.3 h estimated for the M2; 137–156 tok/s, peak
10.1 GB, no restarts), all evals by 22:30. Hold-out 61, but fixed 142 and complex 85 below v15-q4 → not promoted; see
[complex_v17](../sql2pyspark-mlx/complex_v17/README.md).

## Phase 6 — wrap-up (after v17) — done 2026-11-12 except item 3

1. Docs in one pass: README / PROJECT_DOCUMENTATION / docs 01, 03, 05, 07, 08 say "base M2 16 GB" in ~21 places; record
   the new machine, the measured speed and memory, any MLX version change and the phase 5 results; mark this
   document done. **Done 2026-11-12**: the current-state lines name the Mac mini; numbers measured on the M2 stay as
   history ("measured on a base M2").
2. Claude Code memory: replace the M2 hardware notes with the measured M6 numbers. **Done 2026-11-12.**
3. The M2: keep it as a fallback for a few weeks. Wiping or reusing it is the owner's decision (open; so is removing
   `~/m2-backup` on the server).

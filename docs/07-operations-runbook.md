# 07 - Operations Runbook & How-To Guide

A practical guide for setting up, running, changing, and troubleshooting the SQL → PySpark pipeline.

---

## 🛠️ Prerequisites & Local Environment

### System Requirements:
- **Hardware**: Apple Silicon Mac. Developed and measured on a base **M2 with 16 GB** unified memory; more memory gives more headroom.
- **Disk space**: after the 2026-09-30 cleanup the project uses ~15 GB (8.1 GB GGUF, 2 GB venv, 0.6 GB `llama.cpp/`, data and adapters) plus ~16 GB in Ollama (`latest`/v8 and v7). A 7B export needs ~30 GB more while it runs (BF16 `sql2pyspark-hf/` ~15 GB, fused MLX model ~4 GB, new GGUF 8 GB, LoRA checkpoints ~3 GB); `make clean` frees it afterwards. Each new Ollama tag adds 8.1 GB (`ollama rm` old ones).
- **Docker Desktop** (optional): for the containerised app, data pipeline and evaluation; see [Docker](#-docker).

### Software Dependencies:
1. **Java 17** (required by PySpark for the execution checks and evaluation):
   ```bash
   brew install openjdk@17
   # if Java isn't found automatically:
   export JAVA_HOME=$(brew --prefix openjdk@17)
   ```
2. **Ollama**: install from [ollama.com](https://ollama.com) and make sure it's running:
   ```bash
   open -a Ollama
   ```
3. **Python**: tested with Python 3.14 on macOS arm64.

---

## 🚀 One-Line Setup

Create the virtual environment in `sql2pyspark-mlx/.venv` and install the pinned dependencies:

```bash
make setup
```

`make help` lists all targets.

---

## 🔄 End-to-End Pipeline Execution

All workflows are in the [Makefile](../Makefile):

### Phase 1: Synthesize & Verify Dataset
```bash
# Generate 20,000 pairs (v1-v3 families + the v4/v5 families in advanced.py) + 105 hand-written pairs,
# verify in Spark, and split (~20 min natively with 4 workers). SEED picks the generator seed (v7: 7; v5: 5);
# EXCLUDE (default ../eval/test_fixed.jsonl) keeps the fixed test set out of every split.
make data N=20000 SEED=7 WORKERS=4

# Or step by step:
make generate   # writes raw SQL/PySpark pairs to sql2pyspark-mlx/raw_pairs/
make verify     # runs both sides of every pair in local Spark (5 worker processes by default)
make split      # signature-based split into sql2pyspark-mlx/data/
```

*Custom generation size or worker count:*
```bash
make data N=20000 WORKERS=6
```

### Phase 2: LoRA Fine-Tuning
```bash
# MLX LoRA fine-tuning via train_resumable.py
# (v13: 7B continued from v12 on targeted_v13/data_mix (scalar subqueries + replay), 2,400 steps, 5 h 09 min; not used;
#  v12: 7B continued from v11 on targeted_v12/data_mix (OR / mixed AND-OR + replay), 2,400 steps, 4 h 52 min;
#  v11: 7B continued from v10 on targeted_v11/data_mix (correlated subquery cross conditions, set operations, LATERAL VIEW filters), 4,000 steps, 6 h 04 min;
#  v10: 7B continued from v9 on classic_v10/data_mix (classic mirror + IN / NOT IN), 2,400 steps, 6 h 24 min;
#  v9: 7B continued from v8 on complex_v9/data_mix with mirror window style, 2,400 steps, 10 h 18 min;
#  v8: 7B continued from v7 on complex_v8/data_mix, 3,200 steps, 11 h 42 min;
#  v7: 7B continued from v6, 2,400 steps, 4 h 58 min;
#  v6: 7B fresh adapters, 4,800 steps, 8 h 19 min).
# Stop Docker (docker compose down, quit Docker Desktop) and unload Ollama models (ollama stop <model>) first.
# Before re-training, back up the current run (e.g. to sql2pyspark-mlx/runs/v8/): adapters/, data/, raw_pairs/, train.log
make train
```

*Follow the training log:*
```bash
tail -f sql2pyspark-mlx/train.log
```
To continue from an earlier model instead of starting from scratch (v5 continued from v4; v7 continues from v6; v8 from v7; v9 from v8; v10 from v9; v11 from v10; v12 from v11), set `resume_adapter_file` in `lora_config.yaml` to that run's `adapters.safetensors` and use a lower peak learning rate (e.g. 5e-5); the adapter shape (rank, layers) must match.

*Automated unattended pipeline:*
- **v7**: `zsh eval/start_training_v7.sh` (data build wait, train, post-train evals).
- **v8**: `zsh eval/start_training_v8.sh` (checks `complex_v8/data_mix/` and `runs/v7/`, trains with `complex_v8/lora_config.yaml`, runs `eval/post_training_v8.sh`).
- **v9**: `zsh eval/start_training_v9.sh` end-to-end automated runner. It verifies `complex_v9/data_mix/`, stops Docker/Ollama, runs `train_resumable.py` (adapters in `complex_v9/adapters/`), exports both Q8_0 (`sql2pyspark.gguf`) and Q4_K_M (`sql2pyspark-q4.gguf`), registers Ollama tags (`sql2pyspark:v9`, `sql2pyspark:v9-q4`), and benchmarks across all evaluation suites.
- **v10**: `zsh eval/start_training_v10.sh`, the same flow for `classic_v10/data_mix` (adapters in `classic_v10/adapters/`; tags `sql2pyspark:v10`, `:v10-q4`); it overwrites `sql2pyspark.gguf` / `sql2pyspark-q4.gguf` (v9 stays as Ollama tags) and also scores v10-q4 and v9-q4 on the 50 held-out `IN` / `NOT IN` pairs (`eval/results/inlist/`). Log `eval/start_training_v10.log`; training log `sql2pyspark-mlx/classic_v10/train_v10.log`. A finished training run is skipped on restart (`exit=0` in the log).
- **v11**: `zsh eval/start_training_v11.sh`, end-to-end runner for `targeted_v11/data_mix` (resumes v10 adapters from `classic_v10/adapters/`, writes `targeted_v11/adapters/`; exports Q8_0 as `sql2pyspark:v11` and Q4_K_M as `sql2pyspark:v11-q4`). Evaluates `v11-q4` across all 4 suites, the 100-sample complex_v8 test, the 50 IN/NOT IN pairs, and evaluates `v11-q4`, `v10-q4`, and `v9-q4` on the 300 held-out `targeted_v11` pairs (`eval/results/v11-heldout/`). Log `eval/start_training_v11.log`; training log `sql2pyspark-mlx/targeted_v11/train_v11.log`.
- **v12**: `zsh eval/start_training_v12.sh`, the same flow for `targeted_v12/data_mix` (resumes `targeted_v11/adapters`, writes `targeted_v12/adapters/`; tags `sql2pyspark:v12`, `:v12-q4`); scores v12-q4 on every suite and the v11 held-out pairs, and v12-q4 / v11-q4 / v9-q4 on the v12 held-out pairs (`eval/results/v12-heldout/`). Note: the log contains `stopping Docker` at the start, so check for `failed; stopping` / `missing; stopping`, not `stopping`, when watching it.
- **v13**: `zsh eval/start_training_v13.sh` (targeted_v13, resumes v12; tags `sql2pyspark:v13`, `:v13-q4`). Its export overwrites `sql2pyspark.gguf` / `sql2pyspark-q4.gguf`; v13 regressed, so they were restored to v12 from the Ollama blobs: `ollama show --modelfile sql2pyspark:v12-q4 | grep ^FROM` gives the blob path, `cp <blob> sql2pyspark-q4.gguf` (same for `:v12` → `sql2pyspark.gguf`).

`make train` prints to the terminal; to keep a log file, run `make train > sql2pyspark-mlx/train.log 2>&1`. If Metal aborts a step, `train_resumable.py` restarts from the newest checkpoint (saved every 100 or 200 iterations) and continues the learning-rate schedule; its messages start with `[train_resumable]`.

### Phase 3: Export, Fuse & Quantize
```bash
# Fuse adapters, dequantize to BF16, convert to Q8_0 GGUF, register in Ollama
make export TAG=v9

# Or step by step:
make fuse           # 4-bit MLX model + dequantized BF16 copy in sql2pyspark-hf/
make gguf           # llama.cpp convert_hf_to_gguf.py -> sql2pyspark.gguf (Q8_0, 8.10 GB)
make ollama TAG=v9  # registers the GGUF as sql2pyspark:v9

# Optional: 4-bit build for the CPU server (4.68 GB). llama-quantize needs an f16/f32 source:
#   python llama.cpp/convert_hf_to_gguf.py sql2pyspark-hf --outtype f16 --outfile sql2pyspark-f16.gguf
# (build the tool once: pip install cmake; cmake -S llama.cpp -B llama.cpp/build && cmake --build llama.cpp/build --target llama-quantize)
./llama.cpp/build/bin/llama-quantize sql2pyspark-f16.gguf sql2pyspark-q4.gguf Q4_K_M
ollama create sql2pyspark:v9-q4 -f <(sed 's/sql2pyspark.gguf/sql2pyspark-q4.gguf/' Modelfile)

# Once it is in Ollama: delete sql2pyspark-hf/, the fused MLX model, LoRA checkpoints and caches (~20 GB);
# keeps sql2pyspark.gguf, sql2pyspark-q4.gguf, and the final adapters.safetensors
make clean
```

### Phase 4: Benchmark & Evaluate
```bash
# Compare the new model with the previous one on the hold-out set, the v4 hold-out set, the real-world queries
# and 150 test queries (~2 h in total; on 7B, use --timeout 600)
make eval TAG=v10-q4 PREV=v9-q4
```

*Update the README results table:*
```bash
python eval/report.py --readme README.md
```

### Phase 5: Promote to Production
```bash
# Point sql2pyspark:latest at the next model (keeps the old latest as :<PREV> if that tag doesn't exist yet).
# Currently: sql2pyspark:latest is v12-q4 on the Mac (since 2026-10-03 06:05) and on the server (since 2026-10-03 16:48).
make promote TAG=v12-q4 PREV=v11-q4
```
Promote only if the new model scores at least the previous one on the hold-out set, keeps the classic-family accuracy, beats it on the v4 hold-out set, gets a real-world query right, and doesn't regress on the fixed test set (details in [Evaluation](04-evaluation-and-benchmarking.md#promotions)). Roll back anytime with `ollama cp sql2pyspark:<PREV> sql2pyspark:latest`.

The Makefile defaults are `TAG=v9 PREV=v8` (`BASE=mlx-community/Qwen2.5-Coder-7B-Instruct-4bit`).

### Phase 6: Launch Web Interface
```bash
# Streamlit UI at http://localhost:8501
make app
```

---

## ⚡ Memory & Performance Tips

### Don't run Ollama while training
Training peaks at ~9.7–10.3 GB (v6 7B: 10.1 GB). On a 16 GB Mac, running an Ollama evaluation at the same time crashed training with a Metal out-of-memory error. Run evaluations, the app, and Ollama chats only after training finishes.

Docker Desktop's VM (8 GB) competes for the same memory, so stop it before training: `docker compose down`, then quit Docker Desktop.

### Training Speed vs. Memory (`sql2pyspark-mlx/lora_config.yaml`)
Measured on a base M2:

| Setting | Speed | Peak memory |
| :--- | :--- | :--- |
| 8 layers of 36, batch 4, no gradient checkpointing (v2/v3, samples ≤ 603 tokens) | ~0.17–0.19 it/s | ~9.7 GB |
| 16 layers of 36, batch 4, no gradient checkpointing | ~0.14 it/s | ~13 GB |
| 16 layers of 36, batch 4, `grad_checkpoint: true` | ~0.08 it/s | ~4.5 GB |
| 8 layers of 36, batch 2 × 2 accumulation, `grad_checkpoint: true` (**v4/v5**, samples up to 3.1k tokens) | ~0.09–0.18 it/s (micro-batches) | ~9.8–10.3 GB |
| 8 layers of 28, batch 1 × 4 accumulation, `grad_checkpoint: true` (**v6**, 7B base model, 3.2k tokens) | ~0.14–0.25 it/s (micro-batches of 1; ~37–50 tok/s) | 10.1 GB |
| 8 layers of 28, batch 2 × 2 accumulation, `grad_checkpoint: true` (7B dry run, 64 longest samples) | — | Metal out-of-memory |

With v4/v5's long samples on 3B, batch 4 ran out of GPU memory even with gradient checkpointing, and batch 2 without it did too. On the 7B model (v6), batch 2 with checkpointing caused Metal OOM during dry runs, whereas batch 1 with 4 accumulation steps peaked at ~9.2 GB, safely inside the ~10.7–12.7 GB Metal limit on a 16 GB Mac. To test a new setting, run a few iterations on the longest samples before a full run.

### Spark Worker Tuning
Each `verify_pairs.py` worker is a separate process with its own local `SparkSession` (~1 GB).
- Default: `WORKERS=5`
- If memory is tight, use fewer:
  ```bash
  make verify WORKERS=2
  ```

---

## 🐳 Docker

Everything except training and fusing runs in Docker (tested with Docker Desktop 29.8.0 on macOS):

- **Files**: `docker-compose.yml`, `docker/app.Dockerfile`, `docker/tools.Dockerfile` (both on `python:3.12-slim-bookworm`, which still ships `openjdk-17`), `docker/requirements-*.txt`, `docker/ollama-entrypoint.sh`, `.dockerignore` (allowlist, so the ~28 GB of models and data stay out of builds).
- **Services**:
  - `app`: Streamlit UI on port 8501. It uses Ollama on the host through `OLLAMA_URL=http://host.docker.internal:11434`. Java 17 + PySpark (for the optional Kimi K3 repair) make the image 2.1 GB; `KIMI_API_KEY` comes from `.env` through `env_file` (optional), never from the image.
  - `tools` profile: Java 17 + PySpark for the data pipeline and evaluation, with the project mounted at `/workspace` (outputs land in the normal place on the host).
  - `ollama` profile: Ollama in a container on port 11435, registering the model from the mounted GGUF.
- **Limits on a Mac**: containers can't use the Apple GPU. MLX training (`make train`, `make fuse`) must run natively, and Ollama in a container is CPU-only (much slower), so native Ollama is the recommended way to serve the model on macOS. The `ollama` profile is meant for Linux servers.

```bash
docker compose up -d --build                                          # UI on http://localhost:8501, model from the host's Ollama
docker compose --profile tools run --rm tools make data PY=python     # dataset pipeline in a Spark container
docker compose --profile tools run --rm tools make eval PY=python TAG=v4 PREV=v3   # evaluation against the host's Ollama
docker compose --profile tools run --rm tools python eval/evaluate.py --model sql2pyspark:v3 --set eval/holdout.jsonl
OLLAMA_URL=http://ollama:11434 docker compose --profile ollama up -d --build      # Ollama in a container
docker compose down
```

What has been verified:

| Path | Result |
| :--- | :--- |
| `app` container | Builds, reports healthy, converts queries through the host's Ollama; with the Kimi K3 repair (2026-09-28): key passed in from `.env`, empty-table check runs in the container's Spark |
| `tools`: `make data` | Built the v3 dataset: 15,105/15,105 pairs verified in 561.6 s (`sql2pyspark-mlx/data_v3_docker.log`) |
| `tools`: evaluation | v3 on the hold-out set: 59/62 (95.2%), identical replies and scores to the native run (`eval/results/v3__holdout.json`), 3.3 s/query, 3 min 47 s in total |
| `ollama` profile | Not benchmarked (CPU-only on a Mac) |

**Before training**, stop Docker (`docker compose down`, then quit Docker Desktop): its VM takes ~8 GB, which a 16 GB Mac needs for MLX.

**After starting Docker Desktop**, wait until `docker info` succeeds before running compose commands. Docker Desktop can update itself on restart (29.6.2 → 29.8.0 here) and return 500/EOF errors for several minutes meanwhile; the evaluation can always run natively instead (`make eval`).

---

## 🌐 Hosting on a server

To serve the trained model from an Ubuntu server (the public site https://sql2pyspark.praponb.com), follow [Server Deployment](08-server-deployment.md): copy the GGUF, register it in a native Ollama, run the app as `sql2pyspark.service` (`deploy/`), and publish it through a Cloudflare Tunnel behind a Cloudflare Access login. After promoting a new model on the Mac, `zsh deploy/update_model.sh <TAG> <PREV>` puts it on the server (or `GGUF=sql2pyspark-q4.gguf zsh deploy/update_model.sh <TAG> <PREV>` to push a 4-bit quantized model for faster CPU inference and lower memory usage).

## 🩺 Troubleshooting & FAQ

### 1. "Cannot connect to Ollama" in Streamlit
- **Cause**: Ollama isn't running, or `OLLAMA_URL` points to the wrong server.
- **Fix**: `open -a Ollama` (or `ollama serve`), then check `curl http://localhost:11434/api/tags`.

### 2. PySpark fails because Java is missing
- **Cause**: Java 17 isn't installed or isn't on `PATH`.
- **Fix**:
  ```bash
  brew install openjdk@17
  export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
  export PATH="$JAVA_HOME/bin:$PATH"
  ```

### 3. Model output has an unterminated code fence
- **Behavior**: generation stopped before the closing triple backticks.
- **Handled**: `extract_code()` in `app.py` and `eval/evaluate.py` strips a dangling opening fence.

### 4. Editor shows "Cannot find module `streamlit`" (or other imports)
- **Cause**: the Pyrefly type checker is using the system Python instead of the project venv.
- **Fix**: [`pyrefly.toml`](../pyrefly.toml) at the project root points it at `sql2pyspark-mlx/.venv/bin/python`. If errors linger, reload the editor window.

### 5. Training crashed with `[METAL] ... Insufficient Memory`
- **Cause**: another GPU user (usually Ollama) was running at the same time.
- **Fix**: stop Ollama models (`ollama stop <model>`), close other heavy apps, and restart `make train`.

### 6. Training crashed with `[METAL] ... Impacting Interactivity`
- **Cause**: macOS aborts a GPU command buffer that runs too long (happened after ~250 iterations of the first v4 run).
- **Fix**: `make train` already uses `train_resumable.py`, which runs with `MLX_MAX_OPS_PER_BUFFER=2` / `MLX_MAX_MB_PER_BUFFER=40` (smaller buffers, ~9% slower) and resumes from the newest checkpoint if the error still happens.

### 7. Docker commands fail with `500 Internal Server Error` or `EOF`
- **Cause**: Docker Desktop is still starting (or updating itself).
- **Fix**: wait until `docker info` succeeds, e.g. `until docker info >/dev/null 2>&1; do sleep 5; done`, or run the step natively.

### 8. Re-running training from scratch
- Remove the old adapters first (this deletes the current LoRA weights):
  ```bash
  rm -rf sql2pyspark-mlx/adapters
  make train
  ```

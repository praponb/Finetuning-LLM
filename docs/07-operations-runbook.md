# 07 - Operations Runbook & How-To Guide

A practical guide for setting up, running, changing, and troubleshooting the SQL → PySpark pipeline.

---

## 🛠️ Prerequisites & Local Environment

### System Requirements:
- **Hardware**: Apple Silicon Mac. Developed and measured on a base **M2 with 16 GB** unified memory; more memory gives more headroom.
- **Disk space**: the project uses ~14 GB (after `archive/` and `mlx-env/` were deleted on 2026-09-27), of which `sql2pyspark-hf/` (~5.8 GB) can be deleted and is recreated by `make fuse`. Budget ~15 GB for a fresh run: data, adapters, fused models, BF16 export and GGUF; each `runs/vN/` backup adds ~0.3 GB.
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
# verify in Spark, and split (~20 min natively with 4 workers). SEED picks the generator seed (v5: 5);
# EXCLUDE (default ../eval/test_fixed.jsonl) keeps the fixed test set out of every split.
make data N=20000 SEED=5 WORKERS=4

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
# (v7: 7B model continued from v6, 2,400 steps, 4 h 58 min; v6: 7B model fresh adapters, 4,800 steps, 8 h 19 min; v5: continued from v4's 3B adapters with peak LR 5e-5, ~5 h 40 min).
# Stop Docker (docker compose down, quit Docker Desktop) and unload Ollama models (ollama stop <model>) first.
# Before re-training, back up the current run (e.g. to sql2pyspark-mlx/runs/v6/): adapters/, data/, raw_pairs/, train.log
make train
```

*Follow the training log:*
```bash
tail -f sql2pyspark-mlx/train.log
```
To continue from an earlier model instead of starting from scratch (v5 continued from v4; v7 continues from v6), set `resume_adapter_file` in `lora_config.yaml` to that run's `adapters.safetensors` (e.g. `./runs/v6/adapters/adapters.safetensors`) and use a lower peak learning rate (e.g. 5e-5); the adapter shape (rank, layers) must match.

*Automated unattended pipeline:*
You can orchestrate unattended execution via `zsh eval/start_training_v7.sh`: it waits for dataset verification and Kimi repair, terminates Docker and Ollama models to free GPU RAM, starts `make train`, and calls `eval/post_training_v7.sh` upon completion.

`make train` prints to the terminal; to keep a log file, run `make train > sql2pyspark-mlx/train.log 2>&1`. If Metal aborts a step, `train_resumable.py` restarts from the newest checkpoint (saved every 100 or 200 iterations) and continues the learning-rate schedule; its messages start with `[train_resumable]`.

### Phase 3: Export, Fuse & Quantize
```bash
# Fuse adapters, dequantize to BF16, convert to Q8_0 GGUF, register in Ollama
make export TAG=v8

# Or step by step:
make fuse           # 4-bit MLX model + dequantized BF16 copy in sql2pyspark-hf/
make gguf           # llama.cpp convert_hf_to_gguf.py -> sql2pyspark.gguf
make ollama TAG=v8  # registers the GGUF as sql2pyspark:v8
```

### Phase 4: Benchmark & Evaluate
```bash
# Compare the new model with the previous one on the hold-out set, the v4 hold-out set, the real-world queries
# and 150 test queries (~2 h in total; on 7B, use --timeout 600)
make eval TAG=v8 PREV=v7
```

*Update the README results table:*
```bash
python eval/report.py --readme README.md
```

### Phase 5: Promote to Production
```bash
# Point sql2pyspark:latest at the next model (keeps the old latest as :v7 if that tag doesn't exist yet).
# Always pass TAG and PREV: with the defaults alone it re-promotes the Makefile's TAG.
make promote TAG=v8 PREV=v7
```
Promote only if the new model scores at least the previous one on the hold-out set, keeps the classic-family accuracy, beats it on the v4 hold-out set, gets a real-world query right, and doesn't regress on the fixed test set (details in [Evaluation](04-evaluation-and-benchmarking.md#promotions)). v5 was promoted by the owner's decision; v6 (7B) was promoted on 2026-09-28 and v7 on 2026-09-29, each meeting every condition except the real-world one that no model has met (v7 also only ties v6 on the v4 hold-out). Roll back anytime with `ollama cp sql2pyspark:v6 sql2pyspark:latest` (or any other tag).

The Makefile defaults are `TAG=v8 PREV=v7` (`BASE=mlx-community/Qwen2.5-Coder-7B-Instruct-4bit`), i.e. the next model; until `sql2pyspark:v8` exists a bare `make promote` fails instead of re-promoting an old tag. Earlier runs used `TAG=v7 PREV=v6`, `TAG=v6 PREV=v5`, `TAG=v5 PREV=v3` and `TAG=v4 PREV=v3`.

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

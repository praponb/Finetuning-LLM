# 07 - Operations Runbook & How-To Guide

A practical guide for setting up, running, changing, and troubleshooting the SQL → PySpark pipeline.

---

## 🛠️ Prerequisites & Local Environment

### System Requirements:
- **Hardware**: Apple Silicon Mac. Developed and measured on a base **M2 with 16 GB** unified memory; more memory gives more headroom.
- **Disk space**: the project currently uses ~28 GB. About 20 GB of that can be freed: `archive/` (~14 GB), `sql2pyspark-hf/` (~5.8 GB, recreated by `make fuse`) and `mlx-env/` (~0.4 GB, unused). Budget ~15 GB for a fresh run: data, adapters, fused models, BF16 export and GGUF.
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
# Generate 15,000 pairs + 105 hand-written pairs, verify in Spark, and split (~10–15 min)
make data

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
# MLX LoRA fine-tuning (~1 h 50 min – 2 h on a base M2). Stop Docker and Ollama models first.
# Before re-training, back up the current run (e.g. to sql2pyspark-mlx/runs/v3/): adapters/, data/, raw_pairs/, train.log
make train
```

*Follow the training log:*
```bash
tail -f sql2pyspark-mlx/train.log
```
`make train` prints to the terminal; to keep a log file, run `make train > sql2pyspark-mlx/train.log 2>&1`.

### Phase 3: Export, Fuse & Quantize
```bash
# Fuse adapters, dequantize to BF16, convert to Q8_0 GGUF, register in Ollama
make export TAG=v4

# Or step by step:
make fuse           # 4-bit MLX model + dequantized BF16 copy in sql2pyspark-hf/
make gguf           # llama.cpp convert_hf_to_gguf.py -> sql2pyspark.gguf
make ollama TAG=v4  # registers the GGUF as sql2pyspark:v4
```

### Phase 4: Benchmark & Evaluate
```bash
# Compare the new model with the previous one (hold-out set + 300 test queries each; ~1 h in total)
make eval TAG=v4 PREV=v3
```

*Update the README results table:*
```bash
python eval/report.py --readme README.md
```

### Phase 5: Promote to Production
```bash
# Point sql2pyspark:latest at v4 (keeps the old latest as :v3 if that tag doesn't exist yet)
make promote TAG=v4 PREV=v3
```
Promote only if the new model beats the previous one on the hold-out set and doesn't regress on the same test sample. Roll back with `ollama cp sql2pyspark:<previous> sql2pyspark:latest`.

The examples use the next version (v4). Always pass `TAG` and `PREV`: the Makefile defaults are still `TAG=v2 PREV=v1`. The v2 → v3 cycle ran as `make export TAG=v3`, `make eval TAG=v3 PREV=v2` and `make promote TAG=v3 PREV=v2`.

### Phase 6: Launch Web Interface
```bash
# Streamlit UI at http://localhost:8501
make app
```

---

## ⚡ Memory & Performance Tips

### Don't run Ollama while training
Training peaks at ~9.7 GB. On a 16 GB Mac, running an Ollama evaluation at the same time crashed training with a Metal out-of-memory error. Run evaluations, the app, and Ollama chats only after training finishes.

Docker Desktop's VM (8 GB) competes for the same memory, so stop it before training: `docker compose down`, then quit Docker Desktop.

### Training Speed vs. Memory (`sql2pyspark-mlx/lora_config.yaml`)
Measured on a base M2:

| Setting | Speed | Peak memory |
| :--- | :--- | :--- |
| 8 layers, no gradient checkpointing (**current**) | ~0.17–0.19 it/s | ~9.7 GB |
| 16 layers, no gradient checkpointing | ~0.14 it/s | ~13 GB |
| 16 layers, `grad_checkpoint: true` | ~0.08 it/s | ~4.5 GB |

Turn on `grad_checkpoint: true` only if you run out of memory; it makes each step ~40% slower.

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
  - `app`: Streamlit UI on port 8501. It uses Ollama on the host through `OLLAMA_URL=http://host.docker.internal:11434`.
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
| `app` container | Builds, reports healthy, converts queries through the host's Ollama |
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

### 6. Docker commands fail with `500 Internal Server Error` or `EOF`
- **Cause**: Docker Desktop is still starting (or updating itself).
- **Fix**: wait until `docker info` succeeds, e.g. `until docker info >/dev/null 2>&1; do sleep 5; done`, or run the step natively.

### 7. Re-running training from scratch
- Remove the old adapters first (this deletes the current LoRA weights):
  ```bash
  rm -rf sql2pyspark-mlx/adapters
  make train
  ```

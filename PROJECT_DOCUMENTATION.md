# SQL → PySpark Fine-Tuning Pipeline: Master Project Documentation

**An offline, Apple-Silicon pipeline that LoRA-fine-tunes Qwen2.5-Coder-3B-Instruct to translate SQL into idiomatic PySpark DataFrame code.**

This is the single-file version of the guides in [docs/](docs/INDEX.md); each section links to the detailed guide.

---

## 📑 Table of Contents

1. [Executive Summary & Architecture](#1-executive-summary--architecture)
2. [Repository & File Structure](#2-repository--file-structure)
3. [Data Engineering & Synthetic Generation](#3-data-engineering--synthetic-generation)
4. [Spark Dual-Execution Verification](#4-spark-dual-execution-verification)
5. [Leakage-Free Dataset Splitting](#5-leakage-free-dataset-splitting)
6. [LoRA Fine-Tuning & Model Training](#6-lora-fine-tuning--model-training)
7. [Fusion, Dequantization & GGUF Quantization](#7-fusion-dequantization--gguf-quantization)
8. [Ollama Serving & Streamlit Web UI](#8-ollama-serving--streamlit-web-ui)
9. [Execution-Based Evaluation & Benchmarks](#9-execution-based-evaluation--benchmarks)
10. [Operations Runbook & Makefile Reference](#10-operations-runbook--makefile-reference)

---

## 1. Executive Summary & Architecture

The goal is a small local model that turns SQL into PySpark code that actually runs and returns the same result. The pipeline:
1. **Generates ~15k SQL/PySpark pairs** with a compositional generator (`sqlspec.py`, `generate_dataset.py`) that renders both sides from the same objects, so they agree by construction.
2. **Executes every pair in local Spark** (`verify_pairs.py`) and keeps only pairs whose column names, rows and ORDER BY sequence match: all 15,105 v3 pairs pass.
3. **Splits by feature signature** (`convert_sql_pyspark.py`), so validation and test contain feature combinations that never occur in training.
4. **Fine-tunes Qwen2.5-Coder-3B-Instruct (4-bit MLX)** with LoRA on a base M2; validation loss falls from 0.547 to **0.005** (v3).
5. **Exports a Q8_0 GGUF** served by Ollama behind a Streamlit UI.
6. **Scores models by executing their answers** on 62 hand-written queries over unseen tables: **v3 95% correct** (v2 90%, v1 6%). `sql2pyspark:latest` = v3 since 2026-09-26.

### Architecture Diagram:
```mermaid
flowchart LR
    subgraph DataPrep [Data Preparation]
        Schemas[schemas.py] --> Gen[generate_dataset.py]
        HW[handwritten_pairs.py] --> Raw
        Gen --> Raw[raw_pairs/]
        Raw --> SparkCheck[verify_pairs.py<br/>Spark Dual Execution]
        SparkCheck --> Split[convert_sql_pyspark.py<br/>Signature Split]
        Split --> JSONL[data/*.jsonl]
    end

    subgraph TrainPhase [Training & Export]
        JSONL --> MLX[mlx_lm.lora<br/>lora_config.yaml]
        MLX --> Adapters[adapters/]
        Adapters --> Fuse[mlx_lm.fuse --dequantize]
        Fuse --> HF[sql2pyspark-hf/<br/>BF16]
        HF --> Llama[llama.cpp<br/>convert_hf_to_gguf.py]
        Llama --> GGUF[sql2pyspark.gguf<br/>Q8_0 3.3GB]
    end

    subgraph ServeEval [Serving & Validation]
        GGUF --> Modelfile[Modelfile]
        Modelfile --> Ollama[Ollama Server]
        Ollama <--> Streamlit[app.py<br/>Streamlit UI]
        Ollama <--> Eval[eval/evaluate.py<br/>Spark Scoring]
    end
```

Details: [docs/01-architecture-overview.md](docs/01-architecture-overview.md).

---

## 2. Repository & File Structure

```
Finetuning-LLM/
├── Makefile                      # Pipeline orchestration (`make help`)
├── Modelfile                     # Ollama model definition & system prompt
├── README.md                     # Overview, results table, known gaps
├── PROJECT_DOCUMENTATION.md      # This file
├── app.py                        # Streamlit web converter UI
├── requirements.txt              # Pinned dependencies
├── pyrefly.toml                  # Editor type-checker config (project venv)
├── sql2pyspark.gguf              # 3.29 GB Q8_0 v3 model served by Ollama
├── docker-compose.yml, docker/, .dockerignore   # Docker: app, tools (Spark data/eval), ollama
├── docs/                         # Detailed guides (INDEX.md + 01–07)
├── sql2pyspark-mlx/              # Data synthesis, verification & MLX training
│   ├── schemas.py                # 17 train + 5 hold-out tables & sample data
│   ├── sqlspec.py                # Dual-rendering expression/query objects
│   ├── generate_dataset.py       # Compositional query generator (12 families)
│   ├── handwritten_pairs.py      # 105 curated pairs
│   ├── verify_pairs.py           # Parallel PySpark verification
│   ├── convert_sql_pyspark.py    # Signature splitter & chat formatter
│   ├── lora_config.yaml          # MLX LoRA hyperparameters
│   ├── train.log                 # v3 training log
│   ├── data/                     # train.jsonl (12.7k), valid.jsonl, test.jsonl, *_meta.jsonl
│   ├── raw_pairs/                # Generated pairs, verified.jsonl & reports
│   ├── adapters/                 # LoRA weights (+ checkpoints), v3
│   ├── runs/v2/                  # v2 backup: adapters, data, raw pairs, logs
│   └── sql2pyspark-model/        # Fused 4-bit MLX model
├── sql2pyspark-hf/               # Dequantized BF16 fused model (~5.8 GB on disk)
├── eval/                         # Execution-based evaluation
│   ├── build_holdout.py          # 62 hand-written queries on unseen tables
│   ├── holdout.jsonl             # Hold-out benchmark
│   ├── evaluate.py               # Strict/lenient execution evaluator
│   ├── report.py                 # Markdown results-table generator
│   └── results/                  # v1/v2/v3 × holdout/test results (+ v2-split/)
├── llama.cpp/                    # Upstream checkout used for convert_hf_to_gguf.py
├── mlx-env/                      # Legacy venv (unused)
└── archive/                      # Earlier iterations (v1, non-coder trials; ~14 GB)
```

The code is in the private GitHub repo `praponb/Finetuning-LLM`. `.gitignore` keeps model weights, generated data, venvs, `llama.cpp/`, `archive/` and `sql2pyspark-mlx/runs/` out of git; `archive/`, `mlx-env/` and `sql2pyspark-hf/` (recreated by `make fuse`) can be deleted to free ~20 GB.

Details: [docs/06-file-catalog-and-reference.md](docs/06-file-catalog-and-reference.md).

---

## 3. Data Engineering & Synthetic Generation

### A. Schema Catalog (`schemas.py`)
- **17 training tables**: `customers`, `orders`, `order_items`, `products`, `suppliers`, `employees`, `departments`, `accounts`, `transactions`, `sessions`, `web_events`, `payments`, `shipments`, `stores`, `inventory`, `store_sales`, `subscriptions`.
- **5 hold-out tables** (`holdout=True`): `patients`, `doctors`, `appointments`, `flights`, `bookings`. Never used for training data; they test generalisation to unseen schemas.
- **Sample data**: `make_dataframes(spark, seed=7)` builds 40 deterministic rows per table with typed columns (ids, foreign keys, numbers, categories, strings, dates, timestamps, booleans, arrays), including NULLs and some dangling foreign keys.

### B. Dual-Rendering Engine (`sqlspec.py`)
Queries are trees of objects: expressions (`Col`, `Lit`, `BinOp`, `Fn`, `Case`, `Cast`, `Agg`, `WinFn`), predicates (`Cmp`, `InList`, `Between`, `Like`, `IsNull`, `And`, `Or`, `Not`) and query objects (`WinSpec`, `Rel`, `Join`, `Semi`, `Sel`). Every expression implements:
- `.sql(style)`: Spark SQL with randomized surface style (keyword case, layout, aliases, `<>` vs `!=`, ...).
- `.py(ctx)`: the equivalent PySpark following the output conventions.

### C. 12 Query Families (`generate_dataset.py`)
1. `fam_select`: projections, boolean filter trees (`AND`, `OR`, `NOT`, `BETWEEN`, `IN`, `LIKE`, `IS NULL`), computed columns, `DISTINCT`, `ORDER BY` / `LIMIT`.
2. `fam_agg`: `GROUP BY` (columns or expressions), aggregates incl. conditional ones, `HAVING`, `ROLLUP` / `CUBE`.
3. `fam_join`: `INNER`, `LEFT`, `RIGHT`, `FULL OUTER` joins, join chains, self-joins, composite keys.
4. `fam_join_agg`: joins followed by grouping and aggregation.
5. `fam_window`: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTILE`, `PERCENT_RANK`, `LAG` / `LEAD`, partition totals, running and moving aggregates, `FIRST_VALUE` / `LAST_VALUE`.
6. `fam_topn`: top-N per group through a ranked CTE or derived table.
7. `fam_cte_agg_join`: aggregated CTE (or derived table) joined back to a parent table.
8. `fam_cte_chain`: two-step CTE pipelines.
9. `fam_derived`: aggregated subquery in `FROM`, filtered by the outer query.
10. `fam_subquery`: `IN`, `EXISTS`, `NOT EXISTS`, `NOT IN`, scalar and correlated subqueries (semi/anti joins, precomputed values, window aggregates).
11. `fam_setop`: `UNION ALL`, `UNION`, `EXCEPT`, `INTERSECT`.
12. `fam_special`: `PIVOT`, `EXPLODE` / `LATERAL VIEW`, `CROSS JOIN`, window functions over aggregates.

**v3 generator changes** (targeting the v2 hold-out failures):
- Set operations over *differently named* columns (positional, aliased on the right, or aliased on both sides): `.union()` when the names differ, `.unionByName()` when they match.
- `GROUP BY` keys referenced by expression, alias or ordinal, with ordinals more common on `CASE` keys.
- Exact `NOT IN` semantics: `.filter(F.col(x).isNotNull() | F.lit(excluded.isEmpty()))` after the anti join.

### D. 105 Curated Pairs (`handwritten_pairs.py`)
The v1 hand-written set, ported to the v2 schema and conventions. It covers things the generator rarely produces: `%`, `POWER`, `NULLS LAST`, `SPLIT`, `REGEXP_EXTRACT`, `DATEDIFF`, `ADD_MONTHS`, `CROSS JOIN`, cross-table `INTERSECT` / `EXCEPT`, `ROLLUP`, `CUBE`, `PIVOT`. All are execution-verified and always go to training.

Details: [docs/02-data-engineering-pipeline.md](docs/02-data-engineering-pipeline.md).

---

## 4. Spark Dual-Execution Verification

[verify_pairs.py](sql2pyspark-mlx/verify_pairs.py) executes both sides of every pair in local Spark:
$$\text{spark.sql}(SQL) \stackrel{?}{=} \text{exec}(PySpark)$$

### Verification Criteria:
1. **Columns**: names and order must match exactly.
2. **Rows**: normalized rows (numbers rounded to 4 decimals, ISO dates/timestamps, NaN standardized) must match as a multiset (`Counter`).
3. **Order**: if the query has `ORDER BY`, the sequence of the ORDER BY columns must match.

### Verification Run Results (`raw_pairs/verify_report.json`, v3):
- **Checked**: 15,105 pairs
- **Passing**: **15,105 (100%)**; 1,717 of them return an empty result, which makes their check weaker
- **Time**: 561.6 s (~9.4 min) with 5 worker processes (built in the Docker `tools` container, log in `sql2pyspark-mlx/data_v3_docker.log`)
- v2 had one failure (15,104 of 15,105): a `NOT IN` subquery that returns no rows keeps NULL keys in SQL, but the generated PySpark filtered them out. v3 fixes the generator.

---

## 5. Leakage-Free Dataset Splitting

[convert_sql_pyspark.py](sql2pyspark-mlx/convert_sql_pyspark.py) groups verified pairs by **feature signature**:
$$\text{Signature} = \text{family} \;\|\; \text{sorted}(\text{feature\_tags})$$

Whole signatures are held out for validation (10%) and test (5%). A signature is only held out if it's small (≤2% of pairs) and every one of its features keeps at least 30 training examples. The 105 hand-written pairs always go to training.

### Split Statistics (`data/split_stats.json`, v3):
- **Pairs**: 15,105
- **Training (`train.jsonl`)**: 12,743 pairs (3,555 signatures)
- **Validation (`valid.jsonl`)**: 1,575 pairs (359 signatures)
- **Test (`test.jsonl`)**: 787 pairs (168 signatures)
- **Signatures shared with training**: **0**
- **Features missing from training**: **0**

---

## 6. LoRA Fine-Tuning & Model Training

- **Base model**: `mlx-community/Qwen2.5-Coder-3B-Instruct-4bit`
- **Framework**: Apple MLX (`mlx-lm`)
- **Config**: [lora_config.yaml](sql2pyspark-mlx/lora_config.yaml)

### Key Parameters:
- **Adapted layers**: last 8 transformer blocks (of 36)
- **LoRA rank**: 16 | **scale**: 20.0 | **dropout**: 0.05 (6.65 M trainable parameters)
- **Iterations**: 1,200 steps (~0.4 epoch over 12.7k samples at batch size 4)
- **Optimizer**: AdamW, cosine decay (100 warmup steps to $10^{-4}$, decaying to $10^{-5}$)
- **Masked prompt**: `true` (loss only on the PySpark answer)
- **Peak memory**: **9.70 GB**
- **Duration**: ~1 h 50 min – 2 h on a base Apple M2 (16 GB)
- v2 and v3 use the same configuration; `train.log` is the v3 run, the v2 log is in `runs/v2/train.log`.

The original plan was 16 layers and ~3,000 steps, which would have taken ~6.5 h on this machine. Because the data is learned very quickly, the run was shortened.

### Loss Progression (v3):
```
Iteration    Validation Loss    Training Loss    Learning Rate
──────────────────────────────────────────────────────────────
1            0.547              —                —
25           —                  0.185            2.476e-5
50           —                  0.068            4.951e-5
300          0.013              0.017            9.299e-5
600          0.008              0.014            6.166e-5
900          0.005              0.004            2.573e-5
1200         0.005              0.007            1.000e-5
```

v2 ended at val loss 0.004 on its own (different) validation set.

Note: don't run Ollama or Docker while training on a 16 GB Mac; Ollama plus training caused a Metal out-of-memory crash, and Docker Desktop's VM takes another 8 GB.

Details: [docs/03-model-training-and-export.md](docs/03-model-training-and-export.md).

---

## 7. Fusion, Dequantization & GGUF Quantization

To package the fine-tuned model for Ollama (`make export TAG=v3`):

1. **4-bit MLX fusion**:
   `mlx_lm.fuse --model ... --adapter-path adapters --save-path sql2pyspark-model`
   (1.74 GB model for the MLX runtime).
2. **BF16 dequantization**:
   `mlx_lm.fuse --model ... --adapter-path adapters --save-path ../sql2pyspark-hf --dequantize`
   (~6.2 GB BF16 safetensors, 5.8 GiB on disk).
3. **Q8_0 GGUF conversion**:
   `python llama.cpp/convert_hf_to_gguf.py sql2pyspark-hf --outtype q8_0 --outfile sql2pyspark.gguf`
   (**3.29 GB**, ~80 s).
4. **Ollama registration and promotion**:
   `ollama create sql2pyspark:v3 -f Modelfile`, then `make promote TAG=v3 PREV=v2` (`sql2pyspark:latest` → v3, ID `aa95ad200568`; `sql2pyspark:v2` and `:v1` kept). Roll back with `ollama cp sql2pyspark:v2 sql2pyspark:latest`.

---

## 8. Ollama Serving & Streamlit Web UI

### Ollama Serving:
- Serves completions at `http://localhost:11434/api/chat`; `app.py` and `eval/evaluate.py` read `OLLAMA_URL` to use another server.
- Chat template, stop token, sampling parameters and system prompt are in the [Modelfile](Modelfile).

### Streamlit Web Application (`app.py`):
- **Two columns**: SQL input on the left, generated PySpark on the right (with a copy button).
- **Model tag field in the sidebar**: any Ollama tag, e.g. `sql2pyspark:latest` (v3), `sql2pyspark:v3`, `sql2pyspark:v2`, `sql2pyspark:v1`.
- **Robust code extraction**: `extract_code()` also handles replies that stop before the closing code fence.
- **Error handling**: clear messages when Ollama is unreachable or the input is blank; 120 s timeout.
- **Run**: `make app` → `http://localhost:8501`, or `docker compose up -d --build` for the same UI in a container (it uses the host's Ollama).

Details: [docs/05-serving-and-streamlit-ui.md](docs/05-serving-and-streamlit-ui.md).

---

## 9. Execution-Based Evaluation & Benchmarks

The suite in `eval/` scores models by executing their answers in Spark and comparing them with the SQL result.

### Benchmark Sets:
1. **Hold-out set (`eval/holdout.jsonl`)**: 62 hand-written queries (15 simple, 22 medium, 25 complex) on unseen tables (`patients`, `doctors`, `appointments`, `flights`, `bookings`).
2. **Test split (`sql2pyspark-mlx/data/test_meta.jsonl`)**: a random sample of 300 generated queries whose feature signatures never occur in training. The split is recomputed for each dataset, so models are compared on the same sample of the current (v3) split.
3. **Real-world queries (`eval/real_queries.jsonl`)**: SQL supplied by users, on hand-made sample tables in `eval/real_tables.py` (`--tables eval/real_tables.py`). Currently 1 query: a 4-CTE marketing-attribution report.

**Strict** scoring runs the code as returned (it must assign `result`); **lenient** scoring adds imports, binds a bare `df`, and takes the last expression as the result, to be fair to v1.

### Results:
| Model | Eval Set | n | Syntax OK | Runs (strict) | Correct (strict) | Runs (lenient) | Correct (lenient) | s / query |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `sql2pyspark:v1` | holdout | 62 | 79% | 0% | **0%** | 16% | **6%** | 4.2 |
| `sql2pyspark:v2` | holdout | 62 | 98% | 92% | **90%** | 92% | **90%** | 4.1 |
| `sql2pyspark:v2` | test_meta (v3 split) | 300 | 99% | 99% | **95%** | 99% | **95%** | 6.0 |
| `sql2pyspark:v3` | holdout | 62 | 97% | 95% | **95%** | 95% | **95%** | 3.3 |
| `sql2pyspark:v3` | test_meta (v3 split) | 300 | 100% | 98% | **95%** | 98% | **95%** | 5.3 |
| `sql2pyspark:v2` | real_queries | 1 | 0% | 0% | **0%** | 0% | **0%** | 44.0 |
| `sql2pyspark:v3` | real_queries | 1 | 0% | 0% | **0%** | 0% | **0%** | 48.8 |

Hold-out by difficulty (correct, lenient): v3 **100% / 95% / 92%** (simple / medium / complex); v2 93% / 95% / 84%; v1 7% / 14% / 0%.

On the same 300 test queries v2 and v3 are tied (286 vs. 284 correct). Earlier results on the v2 split (v1 8%, v2 98%) are kept in `eval/results/v2-split/` and are not comparable.

**Promotion**: v3 beat v2 on the hold-out set (fixing 3 of v2's 6 failures with no regressions) and tied on the test sample, so it was promoted to `sql2pyspark:latest` on 2026-09-26.

**Remaining v3 failures on the hold-out set (3)**: `UNION` of differently named columns still uses `unionByName` (1), a misplaced parenthesis in an `EXISTS` → `left_semi` join (1), garbled alias syntax in a `UNION ALL` with literal columns (1).

**Real-world query (v4 target)**: both v2 and v3 fail the user's marketing-attribution query; neither reply parses. It is 2,371 tokens as a training sample (training: max 603) and uses `GROUPING SETS` and `INTERVAL` (0 training samples), `NULLS LAST` (1), `NULLIF` (5) and 4 CTEs (training: at most 2).

**Shared gap (v4 target)**: all 9 set-operation queries in the test sample share one held-out signature (`UNION` + literal `source` column + `ORDER BY`); both models drop `.distinct()` after `unionByName` (v2 3/9, v3 1/9 correct).

Details: [docs/04-evaluation-and-benchmarking.md](docs/04-evaluation-and-benchmarking.md).

---

## 10. Operations Runbook & Makefile Reference

| Target | Command | What it does |
| :--- | :--- | :--- |
| `make help` | — | Lists the targets |
| `make setup` | `python3 -m venv sql2pyspark-mlx/.venv && pip install -r requirements.txt` | Creates the environment |
| `make data` | `make generate verify split` | Generates ~15.1k pairs, verifies them in Spark, splits by signature (~10–15 min) |
| `make train` | `mlx_lm.lora -c lora_config.yaml` (in `sql2pyspark-mlx/`) | Fine-tunes on Apple Silicon (~1 h 50 min – 2 h) |
| `make export TAG=v3` | `make fuse gguf ollama` | Fuses adapters, dequantizes, builds the Q8_0 GGUF, registers it in Ollama |
| `make eval TAG=v3 PREV=v2` | `python eval/evaluate.py ...` (×4) | Evaluates both models on the hold-out set and 300 test queries |
| `make promote TAG=v3 PREV=v2` | `ollama cp sql2pyspark:$(TAG) sql2pyspark:latest` | Makes the model the default, keeping the previous one as `:$(PREV)` |
| `make app` | `streamlit run app.py` | Starts the web UI at `http://localhost:8501` |

Always pass `TAG`/`PREV`: the Makefile defaults are still `TAG=v2 PREV=v1`.

**Docker** works for everything except training: `docker-compose.yml` defines an `app` service (Streamlit, using the host's Ollama), a `tools` profile (Java 17 + PySpark for the data pipeline and evaluation) and an optional `ollama` profile; both images use `python:3.12-slim-bookworm`. Verified: the app container, the v3 dataset build (`make data` in `tools`) and the v3 hold-out evaluation in `tools` (59/62, identical to the native run). MLX training can't run in Docker on a Mac, and Docker should be stopped during training (its VM takes ~8 GB).

Details, memory tips and troubleshooting: [docs/07-operations-runbook.md](docs/07-operations-runbook.md).

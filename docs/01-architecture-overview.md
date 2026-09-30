# 01 - Architecture Overview

The **SQL → PySpark Fine-Tuning Pipeline** is an end-to-end, local system developed on Apple Silicon. It trains, quantizes, packages, and serves a specialized coding language model that converts SQL statements into idiomatic Apache Spark DataFrame Python code.

---

## 🏗️ System Architecture & Workflow

The whole lifecycle—from synthetic query generation to inference in a web UI—runs offline on local hardware (developed and measured on a base Apple M2 with 16 GB).

```mermaid
flowchart TD
    subgraph DataEngineering["1. Data Engineering & Verification"]
        S["schemas.py<br/>(22 Train + 6 Hold-out Tables)"]
        G["generate_dataset.py + advanced.py<br/>+ sqlspec.py (dual renderer)"]
        H["handwritten_pairs.py<br/>(105 Curated Pairs)"]
        Raw["raw_pairs/<br/>(~20.1k SQL + PySpark pairs)"]
        Verify["verify_pairs.py<br/>(Local PySpark Dual Execution)"]
        Split["convert_sql_pyspark.py<br/>(Feature-Signature Split)"]
        Data["data/<br/>train.jsonl (16.9k)<br/>valid.jsonl (2.1k)<br/>test.jsonl (1.0k)"]

        S --> G
        G --> Raw
        H --> Raw
        Raw --> Verify
        Verify -->|20,066 of 20,105 pass (v7)| Split
        Split --> Data
    end

    subgraph Training["2. Local LoRA Fine-Tuning (Apple MLX)"]
        BaseModel["Qwen2.5-Coder Base<br/>v1–v5: 3B-Instruct-4bit<br/>v6–v8: 7B-Instruct-4bit"]
        Config["lora_config.yaml<br/>(Rank 16, 8 Layers, 1200 updates x 4)"]
        MLX["mlx_lm.lora<br/>train_resumable.py"]
        Adapters["adapters/<br/>(adapters.safetensors)"]

        Data --> MLX
        BaseModel --> MLX
        Config --> MLX
        MLX --> Adapters
    end

    subgraph ExportQuantization["3. Model Fusion & Quantization"]
        FuseMLX["mlx_lm.fuse<br/>(4-bit MLX Model)"]
        FuseHF["mlx_lm.fuse --dequantize<br/>(BF16 safetensors)"]
        HFDir["sql2pyspark-hf/<br/>(~15.2 GB BF16)"]
        GGUFConv["llama.cpp<br/>convert_hf_to_gguf.py"]
        GGUF["sql2pyspark.gguf<br/>(8.1 GB Q8_0)"]

        Adapters --> FuseMLX
        Adapters --> FuseHF
        FuseHF --> HFDir
        HFDir --> GGUFConv
        GGUFConv --> GGUF
    end

    subgraph ServingUI["4. Serving & Streamlit UI"]
        Modelfile["Modelfile<br/>(Chat Template & System Prompt)"]
        Ollama["Ollama Engine<br/>(latest = v8 7B; v7, v6, v5, v4, v3 kept)"]
        Streamlit["app.py<br/>(Streamlit Web UI)"]
        KimiApp["kimi_repair.py<br/>(Static check + EmptyTableRunner)"]

        GGUF --> Modelfile
        Modelfile --> Ollama
        Ollama <-->|HTTP API /api/chat| Streamlit
        Streamlit <-->|AST Check & Repair Loop| KimiApp
    end

    subgraph Evaluation["5. Execution-Based Evaluation"]
        Holdout["eval/holdout.jsonl (62 Queries)<br/>+ holdout_v4.jsonl (25 Queries)<br/>+ real_queries.jsonl (4 Queries)<br/>+ test_fixed.jsonl (150 Queries)"]
        EvalRunner["eval/evaluate.py<br/>(Strict & Lenient Spark Scoring)"]
        AgentRepair["eval/agent_repair.py<br/>(Kimi K3 Feedback Loop)"]
        Report["eval/report.py<br/>(Markdown Comparison Tables)"]

        Ollama --> EvalRunner
        Holdout --> EvalRunner
        EvalRunner --> Report
        EvalRunner --> AgentRepair
    end
```

---

## 🎯 Design Principles & Core Philosophy

### 1. Dual-Rendering by Construction (`sqlspec.py`)
Rather than translating SQL with another LLM (which can hallucinate) or with fragile regular expressions, each query is built as a tree of Python objects (expressions, predicates, window specs, relations, `SELECT`s). Each object class in `sqlspec.py` defines both a `.sql(style)` method and a `.py(ctx)` method, so both texts are rendered from the same underlying structure.

### 2. Ground-Truth Spark Execution Verification (`verify_pairs.py`)
Before any pair is used for training, it is executed in a local `pyspark.sql.SparkSession`:
$$\text{spark.sql(sql\_query)} \stackrel{?}{=} \text{exec(pyspark\_code)}$$
Both results are compared on:
- Exact column names and order
- Multiset row equivalence (row order ignored, floats rounded to 4 decimals)
- Sequence of the `ORDER BY` columns, when the query has one

Only matching pairs go into `verified.jsonl`. In the v7 dataset 20,066 of 20,105 pairs pass (20,065 after removing 1 duplicate of the fixed test set); in v5 20,071 passed; in v4 20,068 passed; in v3 all 15,105 passed.

### 3. Feature-Signature Splitting (`convert_sql_pyspark.py`)
Random row-level splitting leaks: near-identical query structures with different constants land in both train and test. Instead:
- Each query gets a **feature signature**: `family | sorted(feature_tags)`.
- Whole signatures are held out for `valid.jsonl` and `test.jsonl` (0 signatures shared with train).
- Validation loss therefore measures generalisation to unseen combinations of SQL features. (It still uses the generator's own style; the hand-written hold-out set in `eval/` is the stronger test.)

### 4. Apple Silicon Training (Apple MLX)
- **Base model**:
  - `v1–v5`: `mlx-community/Qwen2.5-Coder-3B-Instruct-4bit` (36 transformer blocks, 3.09 B parameters)
  - `v6–v7`: `mlx-community/Qwen2.5-Coder-7B-Instruct-4bit` (28 transformer blocks, 7.61 B parameters)
- **Unified memory budget**: fine-tuning runs natively in Apple Silicon unified memory using `mlx-lm`.
  - v2/v3 (3B, short samples): batch 4, no gradient checkpointing, peak ~9.7 GB, ~1 h 50 min.
  - v4/v5 (3B, samples up to 3.1k tokens): batch 2 × 2 gradient accumulation, gradient checkpointing, peak ~9.8–10.3 GB, ~5 h 15 min – 5 h 40 min.
  - v6 (7B, 3.2k tokens): batch 1 × 4 gradient accumulation, gradient checkpointing (batch 2 runs out of GPU memory), peak 10.1 GB, 8 h 19 min on a base M2, ~40–48 trained tokens/s.
  - v7 (7B, continued from v6): batch 1 × 4 gradient accumulation, 2,400 micro-batches (4 h 58 min, peak 9.9 GB), continuing from v6 adapters with halved learning rate.
  - v8 (7B, continued from v7): the standalone `complex_v8` dataset (10,000 complex pairs on 20 new tables) + 7,000 v7 pairs, 3,200 micro-batches (11 h 42 min, peak 10.0 GB); `latest` since 2026-09-30.
- **Resilience**: `train_resumable.py` sets smaller Metal command buffers (`MLX_MAX_OPS_PER_BUFFER=2`, `MLX_MAX_MB_PER_BUFFER=40`) and resumes from the latest checkpoint if macOS aborts a long GPU buffer.

### 5. Hybrid Inference & Empty-Table Verification Loop (`kimi_repair.py`)
To handle complex multi-CTE queries where small models can fail on identifier names, missing closing parentheses, or repetition loops:
- The local fine-tuned model converts the query first (fast, free, private).
- An AST static checker immediately detects syntax errors, undefined variables, missing `result`, and `.unionByName()` used for positional `UNION`/`INTERSECT`/`EXCEPT` where branches have different column names (`setop_check()`).
- When an error is detected, when the SQL is long (> 2,000 characters), or on demand, **Kimi K3** (Moonshot AI) checks and repairs the code using an empty-table Spark runner (`EmptyTableRunner`). Schemas are inferred from the SQL without needing real user data. With updated prompt rules (explicit `UNPIVOT` NULL exclusion and positional set-operation matching), this loop repaired v7's answers to the 4 production-grade queries 23 times out of 24 (3 runs per mode; 12/12 on empty tables) for ~$0.07–$0.27 per query.


### 6. Train on the Mac, serve anywhere
Training needs Apple MLX, so it runs on the M2 Mac; the exported GGUF is portable. Besides the Mac (native Ollama with the Metal GPU, app in Docker), the model is served from an Ubuntu server for the public site https://sql2pyspark.praponb.com: native Ollama on the CPU, the app as a systemd service and a Cloudflare Tunnel for HTTPS. See [Server Deployment](08-server-deployment.md).
---

## 📐 Output Code Conventions (Model Target Behavior)

The model is fine-tuned to produce PySpark code that follows these conventions:

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window  # included only when window functions are used

# 1. Window definitions and other preamble come first
w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc(), "id")

# 2. Input DataFrames are named df_<table>
# 3. The final DataFrame is always assigned to `result`
result = (
    df_employees
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 3)
    .select("id", "full_name", "dept_id", "salary")
)
```

### Key Formatting Rules:
1. **Input names**: table `table_name` is always read from `df_table_name`.
2. **Output name**: the final DataFrame is always assigned to `result`.
3. **Column order & aliases**: output columns, aliases and order match the SQL `SELECT` list one-to-one.
4. **Column references**: `F.col("name")` in expressions; plain strings for simple `.select("a", "b")` / `.groupBy("a")`; joins use `df_x.alias("a")` with `F.col("a.col")`.
5. **No bare counts**: aggregations use `.agg(F.count("*").alias("cnt"))` rather than `.count()`, so the column name matches the SQL alias.
6. **Set operations**:
   - `UNION ALL` → `.unionByName(other)` when both sides have the same column names, otherwise `.union(other)` (SQL matches columns by position)
   - `UNION` → the same call followed by `.distinct()`
   - `EXCEPT` → `.subtract(other)`
   - `INTERSECT` → `.intersect(other)`
7. **CTEs**: become named intermediate DataFrames instead of one nested expression; scalar subqueries become a value computed up front (`avg_x = df.agg(...).first()[0]`).

# SQL → PySpark Fine-Tuning: Documentation Index

Welcome to the technical documentation for the **SQL → PySpark Fine-Tuning Pipeline**. This project implements an end-to-end, local machine learning and data engineering workflow on Apple Silicon to fine-tune **Qwen2.5-Coder-3B-Instruct** for translating Spark SQL queries into idiomatic PySpark DataFrame code.

**Status (2026-09-26):** the v3 model is live as `sql2pyspark:latest`: 95% correct on the unseen-schema hold-out set (v2: 90%, v1: 6%) and 95% on a 300-query sample of the held-out test split (tied with v2). v2 and v1 remain available as `sql2pyspark:v2` / `:v1`. The Docker setup works (app, Spark data pipeline, evaluation). The code is in the private GitHub repo `praponb/Finetuning-LLM`.

---

## 📚 Documentation Suite

| Document | Topic | Target Audience | Key Contents |
| :--- | :--- | :--- | :--- |
| [Architecture Overview](01-architecture-overview.md) | High-Level System Architecture | All Developers | End-to-end pipeline diagram, design principles, output conventions (`df_<table>`, `result = ...`) |
| [Data Engineering Pipeline](02-data-engineering-pipeline.md) | Dataset Synthesis & Verification | Data Engineers, ML Engineers | Table catalog (17 train / 5 hold-out), dual-rendering expression engine (`sqlspec.py`), 12 query families, v3 generator changes, Spark dual-execution check, leakage-free split |
| [Model Training & Export](03-model-training-and-export.md) | MLX LoRA & GGUF Quantization | ML Engineers, MLOps | LoRA hyperparameters, training curve, adapter fusion, BF16 dequantization, llama.cpp Q8_0 export, Ollama packaging |
| [Evaluation & Benchmarking](04-evaluation-and-benchmarking.md) | Execution-Based Scoring | ML Engineers, QA | 62 hold-out queries on unseen schemas, strict vs. lenient scoring, v1/v2/v3 results, promotion rule, failure analysis and v4 targets |
| [Serving & Streamlit UI](05-serving-and-streamlit-ui.md) | Local Inference & Frontend | Frontend Developers, Users | Ollama `/api/chat` integration, `OLLAMA_URL`, Streamlit UI (`app.py`), code-block extraction, model tag field, app in Docker |
| [File Catalog & Reference](06-file-catalog-and-reference.md) | Repository Index | All Contributors | Reference of the scripts, datasets, configs and artifacts in the repository |
| [Operations Runbook](07-operations-runbook.md) | Practical How-To & Makefile Guide | Developers, DevOps | Environment setup, Make targets, memory limits, Docker, troubleshooting |
| [Master Project Documentation](../PROJECT_DOCUMENTATION.md) | All-in-One Guide | All Readers | Single-file summary of everything above |

---

## 🚀 Quick Navigation by Persona

### For Data Engineers
- How queries are generated: [Data Engineering Pipeline § 3](02-data-engineering-pipeline.md#3-compositional-query-generator-generate_datasetpy)
- Table schemas and seed data: [Data Engineering Pipeline § 1](02-data-engineering-pipeline.md#1-table-catalog--schema-definitions-schemaspy)
- The Spark verification engine: [Data Engineering Pipeline § 5](02-data-engineering-pipeline.md#5-spark-dual-execution-verifier-verify_pairspy)

### For Machine Learning Engineers
- LoRA training config & parameters: [Model Training & Export](03-model-training-and-export.md)
- The leakage-free signature split: [Data Engineering Pipeline § 6](02-data-engineering-pipeline.md#6-leakage-free-dataset-splitting-convert_sql_pysparkpy)
- Benchmark metrics and failure analysis: [Evaluation & Benchmarking](04-evaluation-and-benchmarking.md)

### For Developers & Users
- Start the Streamlit interface: [Operations Runbook § Phase 6](07-operations-runbook.md#phase-6-launch-web-interface)
- Serve the model via Ollama: [Serving & Streamlit UI](05-serving-and-streamlit-ui.md)
- Look up specific files: [File Catalog & Reference](06-file-catalog-and-reference.md)

---

## 🧭 Repository At A Glance

```
.
├── Makefile                      # Pipeline runner (`make help` lists targets)
├── Modelfile                     # Ollama model definition & system prompt
├── README.md                     # Project summary and results table
├── PROJECT_DOCUMENTATION.md      # Consolidated single-file documentation
├── app.py                        # Streamlit web converter UI
├── requirements.txt              # Pinned dependencies (MLX, PySpark, Streamlit, ...)
├── pyrefly.toml                  # Editor type-checker config (uses the project venv)
├── sql2pyspark.gguf              # Q8_0 v3 model served by Ollama (3.3 GB)
├── docker-compose.yml            # Docker: app, tools (Spark data/eval) and ollama services
├── docker/                       # Dockerfiles, container requirements, Ollama entrypoint
├── docs/                         # Detailed modular documentation
│   ├── INDEX.md                  # This documentation index
│   ├── 01-architecture-overview.md
│   ├── 02-data-engineering-pipeline.md
│   ├── 03-model-training-and-export.md
│   ├── 04-evaluation-and-benchmarking.md
│   ├── 05-serving-and-streamlit-ui.md
│   ├── 06-file-catalog-and-reference.md
│   └── 07-operations-runbook.md
├── sql2pyspark-mlx/              # Data synthesis, verification & MLX training
│   ├── schemas.py                # Table catalog (17 train / 5 hold-out tables)
│   ├── sqlspec.py                # Dual-rendering expression/query objects
│   ├── generate_dataset.py       # Compositional query generator (12 families)
│   ├── handwritten_pairs.py      # 105 curated pairs
│   ├── verify_pairs.py           # PySpark dual-execution verifier
│   ├── convert_sql_pyspark.py    # Leakage-free splitter & chat formatter
│   ├── lora_config.yaml          # MLX LoRA training hyperparameters
│   ├── train.log                 # Training log of the v3 run
│   ├── data/                     # train.jsonl (12.7k), valid.jsonl, test.jsonl (+ *_meta.jsonl)
│   ├── raw_pairs/                # 15.1k pair files, verified.jsonl & reports
│   ├── adapters/                 # Fine-tuned LoRA weights + checkpoints (v3)
│   ├── runs/v2/                  # v2 backup: adapters, data, raw pairs, logs
│   └── sql2pyspark-model/        # Fused 4-bit MLX model
├── sql2pyspark-hf/               # Dequantized BF16 fused model (input for GGUF)
├── eval/                         # Execution-based evaluation
│   ├── build_holdout.py          # 62 hand-written queries on unseen schemas
│   ├── holdout.jsonl             # Benchmark query set
│   ├── evaluate.py               # Execution-based model evaluation
│   ├── report.py                 # Markdown results-table generator
│   └── results/                  # v1/v2/v3 × holdout/test results (+ v2-split/ for the older test split)
├── llama.cpp/                    # Upstream checkout (GGUF converter)
└── archive/                      # Earlier iterations (v1, non-coder trials; ~14 GB)
```

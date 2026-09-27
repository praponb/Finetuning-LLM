# SQL -> PySpark fine-tuning pipeline.  Run `make help` for the targets.
#
#   make data      generate pairs -> execution-check in Spark -> train/valid/test split
#   make train     LoRA fine-tune (mlx_lm, via train_resumable.py) using sql2pyspark-mlx/lora_config.yaml
#   make export    fuse adapters, dequantize, convert to GGUF, register in Ollama as sql2pyspark:$(TAG)
#   make eval      score sql2pyspark:$(TAG) and the previous model on the hold-out, v4 hold-out, real-query and test sets
#   make promote   make sql2pyspark:$(TAG) the default (`latest`); keeps the old one as :$(PREV)

MLX      := sql2pyspark-mlx
VENV     := $(MLX)/.venv/bin
PY       := $(CURDIR)/$(VENV)/python
BASE     := mlx-community/Qwen2.5-Coder-3B-Instruct-4bit
TAG      ?= v4
PREV     ?= v3
N        ?= 15000
WORKERS  ?= 5
SAMPLE   ?= 300

.PHONY: help setup data generate verify split train fuse gguf ollama export eval promote app

help:
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

setup: ## create the venv and install requirements (needs Python 3.12+ and Java 17 for Spark)
	python3 -m venv $(MLX)/.venv
	$(VENV)/pip install -r requirements.txt

data: generate verify split ## full dataset build

generate: ## write generated + hand-written pairs to sql2pyspark-mlx/raw_pairs/
	cd $(MLX) && rm -rf raw_pairs && $(PY) generate_dataset.py --n $(N) --out raw_pairs \
		&& $(PY) handwritten_pairs.py --out raw_pairs

verify: ## run both sides of every pair in local Spark, keep matching pairs
	cd $(MLX) && $(PY) verify_pairs.py --src raw_pairs --workers $(WORKERS)

split: ## leakage-free train/valid/test split in mlx chat format
	cd $(MLX) && $(PY) convert_sql_pyspark.py --src raw_pairs --out-dir data

train: ## LoRA fine-tune (writes sql2pyspark-mlx/adapters/); resumes by itself if Metal aborts a step
	cd $(MLX) && $(PY) train_resumable.py --config lora_config.yaml

fuse: ## merge adapters -> 4-bit MLX model and a dequantized HF copy for llama.cpp
	cd $(MLX) && $(CURDIR)/$(VENV)/mlx_lm.fuse --model $(BASE) --adapter-path adapters --save-path sql2pyspark-model
	cd $(MLX) && $(CURDIR)/$(VENV)/mlx_lm.fuse --model $(BASE) --adapter-path adapters --save-path ../sql2pyspark-hf --dequantize

gguf: ## convert the HF copy to a Q8_0 GGUF
	$(PY) llama.cpp/convert_hf_to_gguf.py sql2pyspark-hf --outtype q8_0 --outfile sql2pyspark.gguf

ollama: ## register the GGUF in Ollama as sql2pyspark:$(TAG)
	ollama create sql2pyspark:$(TAG) -f Modelfile

export: fuse gguf ollama ## fuse + gguf + ollama

eval: ## execution-based comparison of sql2pyspark:$(TAG) vs sql2pyspark:$(PREV) (4 eval sets)
	$(PY) eval/evaluate.py --model sql2pyspark:$(PREV) --set eval/holdout.jsonl
	$(PY) eval/evaluate.py --model sql2pyspark:$(TAG) --set eval/holdout.jsonl
	$(PY) eval/evaluate.py --model sql2pyspark:$(PREV) --set eval/holdout_v4.jsonl
	$(PY) eval/evaluate.py --model sql2pyspark:$(TAG) --set eval/holdout_v4.jsonl
	$(PY) eval/evaluate.py --model sql2pyspark:$(PREV) --set eval/real_queries.jsonl --tables eval/real_tables.py
	$(PY) eval/evaluate.py --model sql2pyspark:$(TAG) --set eval/real_queries.jsonl --tables eval/real_tables.py
	$(PY) eval/evaluate.py --model sql2pyspark:$(PREV) --set $(MLX)/data/test_meta.jsonl --sample $(SAMPLE)
	$(PY) eval/evaluate.py --model sql2pyspark:$(TAG) --set $(MLX)/data/test_meta.jsonl --sample $(SAMPLE)

promote: ## point sql2pyspark:latest (used by app.py) at $(TAG), keeping the old latest as :$(PREV)
	ollama show sql2pyspark:$(PREV) >/dev/null 2>&1 || ollama cp sql2pyspark:latest sql2pyspark:$(PREV)
	ollama cp sql2pyspark:$(TAG) sql2pyspark:latest

app: ## start the Streamlit UI
	$(VENV)/streamlit run app.py

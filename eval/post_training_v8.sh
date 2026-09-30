#!/bin/zsh
# v8 post-training chain: wait for training on complex_v8/data_mix, export sql2pyspark:v8 from complex_v8/adapters,
# evaluate v8 on the 4 eval sets + a 100-query sample of the complex_v8 test set (v7 too, same sample), restart
# Docker + the app, then run the Kimi K3 repair on v8's real-query answers. Log: eval/post_training_v8.log.
# Promotion (make promote TAG=v8 PREV=v7) is left to the user.
cd /Users/praponb/GitHub/Finetuning-LLM
PY=sql2pyspark-mlx/.venv/bin/python
VENV=/Users/praponb/GitHub/Finetuning-LLM/sql2pyspark-mlx/.venv/bin
BASE=mlx-community/Qwen2.5-Coder-7B-Instruct-4bit
LOG=sql2pyspark-mlx/complex_v8/train_v8.log
step() { echo "[$(date +%H:%M:%S)] $*"; }
until grep -q "^exit=" $LOG; do sleep 30; done
grep -q "^exit=0" $LOG || { step "training failed; stopping"; exit 1; }
[ -s sql2pyspark-mlx/complex_v8/adapters/adapters.safetensors ] || { step "no v8 adapters; stopping"; exit 1; }

step "export v8 (fuse -> dequantized HF -> Q8_0 GGUF -> Ollama)"
(cd sql2pyspark-mlx && $VENV/mlx_lm.fuse --model $BASE --adapter-path complex_v8/adapters --save-path sql2pyspark-model \
  && $VENV/mlx_lm.fuse --model $BASE --adapter-path complex_v8/adapters --save-path ../sql2pyspark-hf --dequantize) \
  || { step "fuse failed"; exit 1; }
$PY llama.cpp/convert_hf_to_gguf.py sql2pyspark-hf --outtype q8_0 --outfile sql2pyspark.gguf || { step "gguf failed"; exit 1; }
ollama create sql2pyspark:v8 -f Modelfile || { step "ollama create failed"; exit 1; }

step "v8 holdout";      $PY eval/evaluate.py --model sql2pyspark:v8 --set eval/holdout.jsonl --timeout 600
step "v8 holdout_v4";   $PY eval/evaluate.py --model sql2pyspark:v8 --set eval/holdout_v4.jsonl --timeout 600
step "v8 real_queries"; $PY eval/evaluate.py --model sql2pyspark:v8 --set eval/real_queries.jsonl --tables eval/real_tables.py --timeout 600
step "v8 test_fixed";   $PY eval/evaluate.py --model sql2pyspark:v8 --set eval/test_fixed.jsonl --timeout 600
for M in v8 v7; do
  step "$M complex_v8 test (100 of 300)"
  $PY eval/evaluate.py --model sql2pyspark:$M --set sql2pyspark-mlx/complex_v8/data/test_meta.jsonl \
    --tables sql2pyspark-mlx/complex_v8/eval_tables.py --sample 100 --seed 0 --timeout 600 \
    --out eval/results/${M}__complex_v8_test.json
done

step "restart Docker + app"; open -a "Docker Desktop"; until docker info >/dev/null 2>&1; do sleep 5; done
docker compose up -d app
step "v8 Kimi repair (sample tables)"; $PY eval/agent_repair.py --results eval/results/v8__real_queries.json > eval/agent_repair_v8.log 2>&1
step "v8 Kimi repair (empty tables)";  $PY eval/agent_repair.py --results eval/results/v8__real_queries.json --mode empty > eval/agent_repair_v8_empty.log 2>&1
step "ALL DONE"

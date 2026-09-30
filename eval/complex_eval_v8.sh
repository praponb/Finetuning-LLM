#!/bin/zsh
# Re-run of the complex_v8 test-set evals (the chain's first attempt failed: dataclass + importlib on Python 3.14 ->
# complex_v8/eval_tables.py wrapper). Waits for the v8 chain, frees memory (Docker down), scores v8 and v7 on the same
# 100-query sample, then restarts Docker + the app. Log: eval/complex_eval_v8.log
cd /Users/praponb/GitHub/Finetuning-LLM
PY=sql2pyspark-mlx/.venv/bin/python
step() { echo "[$(date +%H:%M:%S)] $*"; }
until grep -q "ALL DONE" eval/post_training_v8.log; do sleep 30; done
step "stop Docker for the Ollama evals"
docker compose down; osascript -e 'quit app "Docker Desktop"'
until ! pgrep -f "com.docker.virtualization|Docker Desktop.app/Contents/MacOS" >/dev/null; do sleep 2; done
for M in v8 v7; do
  step "$M complex_v8 test (100 of 300)"
  $PY eval/evaluate.py --model sql2pyspark:$M --set sql2pyspark-mlx/complex_v8/data/test_meta.jsonl \
    --tables sql2pyspark-mlx/complex_v8/eval_tables.py --sample 100 --seed 0 --timeout 600 \
    --out eval/results/${M}__complex_v8_test.json
  for m in $(ollama ps | awk 'NR>1{print $1}'); do ollama stop $m; done
done
step "restart Docker + app"; open -a "Docker Desktop"; until docker info >/dev/null 2>&1; do sleep 5; done
docker compose up -d app
step "COMPLEX EVAL DONE"

#!/bin/zsh
# Export and evaluate an earlier checkpoint of the v14 run (default iter 2400) as sql2pyspark:v14-c<N>-q4 (Q4_K_M only):
# v14-q4 (iter 4800) gained the classic subquery family but slipped on names in unseen tables (hold-out 58 / v12-q4 59,
# v4 18 / 21). Decisive sets first (hold-out, v4, fixed test, v14 held-out); the rest only when it reaches hold-out >= 59.
#   zsh eval/eval_v14_ckpt.sh [N]      log: eval/eval_v14_ckpt.log
cd /Users/praponb/GitHub/Finetuning-LLM
N=${1:-2400}
PY=sql2pyspark-mlx/.venv/bin/python
VENV=/Users/praponb/GitHub/Finetuning-LLM/sql2pyspark-mlx/.venv/bin
BASE=mlx-community/Qwen2.5-Coder-7B-Instruct-4bit
CK=sql2pyspark-mlx/targeted_v14/ckpt$N
M=v14-c$N-q4
step() { echo "[$(date +%H:%M:%S)] $*"; }
unload() { for m in $(ollama ps | awk 'NR>1{print $1}'); do ollama stop $m; done }
SRC=sql2pyspark-mlx/targeted_v14/adapters/$(printf %07d $N)_adapters.safetensors
[ -s $SRC ] || { step "$SRC missing; stopping"; exit 1; }
if docker info >/dev/null 2>&1; then
  step "stopping Docker"; docker compose down; osascript -e 'quit app "Docker Desktop"'
  until ! pgrep -f "com.docker.virtualization|Docker Desktop.app/Contents/MacOS" >/dev/null; do sleep 2; done
fi
unload
if ! ollama list | grep -q "^sql2pyspark:$M "; then
  step "export $M"
  mkdir -p $CK && cp sql2pyspark-mlx/targeted_v14/adapters/adapter_config.json $CK/ && cp $SRC $CK/adapters.safetensors
  rm -rf sql2pyspark-hf
  (cd sql2pyspark-mlx && $VENV/mlx_lm.fuse --model $BASE --adapter-path targeted_v14/ckpt$N --save-path ../sql2pyspark-hf --dequantize) \
    || { step "fuse failed"; exit 1; }
  $PY llama.cpp/convert_hf_to_gguf.py sql2pyspark-hf --outtype f16 --outfile sql2pyspark-f16.gguf >/dev/null 2>&1 || { step "gguf failed"; exit 1; }
  llama.cpp/build/bin/llama-quantize sql2pyspark-f16.gguf sql2pyspark-ckpt-q4.gguf Q4_K_M >/dev/null 2>&1 || { step "Q4_K_M failed"; exit 1; }
  rm -f sql2pyspark-f16.gguf && rm -rf sql2pyspark-hf
  sed 's#^FROM .*#FROM ./sql2pyspark-ckpt-q4.gguf#' Modelfile > Modelfile.ckpt
  ollama create sql2pyspark:$M -f Modelfile.ckpt || { step "ollama create $M failed"; exit 1; }
  rm -f Modelfile.ckpt sql2pyspark-ckpt-q4.gguf
fi
step "$M holdout";    $PY eval/evaluate.py --model sql2pyspark:$M --set eval/holdout.jsonl --timeout 600
step "$M holdout_v4"; $PY eval/evaluate.py --model sql2pyspark:$M --set eval/holdout_v4.jsonl --timeout 600
step "$M test_fixed"; $PY eval/evaluate.py --model sql2pyspark:$M --set eval/test_fixed.jsonl --timeout 600
step "$M targeted_v14 held-out pairs (200)"
$PY eval/evaluate.py --model sql2pyspark:$M --set sql2pyspark-mlx/targeted_v14/data/heldout_meta.jsonl --timeout 600 \
  --out eval/results/v14-heldout/${M}__heldout.json
H=$(python3 -c "import json;print(sum(bool(c['strict'].get('match')) for c in json.load(open('eval/results/${M}__holdout.json'))['cases']))")
step "$M hold-out = $H / 62"
if [ "$H" -ge 59 ]; then
  step "$M complex_v8 test (100 of 300)"
  $PY eval/evaluate.py --model sql2pyspark:$M --set eval/complex_v8_test.jsonl \
    --tables sql2pyspark-mlx/complex_v8/eval_tables.py --sample 100 --seed 0 --timeout 600
  step "$M IN / NOT IN held-out pairs (50)"
  $PY eval/evaluate.py --model sql2pyspark:$M --set sql2pyspark-mlx/classic_v10/data/inlist_valid_meta.jsonl --timeout 600 \
    --out eval/results/inlist/${M}__inlist_valid.json
  step "$M real_queries"; $PY eval/evaluate.py --model sql2pyspark:$M --set eval/real_queries.jsonl --tables eval/real_tables.py --timeout 600
fi
unload
step "restart Docker + app"; open -a "Docker Desktop"; until docker info >/dev/null 2>&1; do sleep 5; done
docker compose up -d app
step "ALL DONE"

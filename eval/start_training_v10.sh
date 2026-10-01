#!/bin/zsh
# v10 launcher + post-training chain. v10 = v9 continued on classic_v10/data_mix (classic pairs in the "mirror" window
# style + IN / NOT IN pairs without SQL aliases).
#   1. check the data, stop Docker and Ollama models (16 GB M2)
#   2. train (classic_v10/lora_config.yaml, resumes complex_v9/adapters, writes classic_v10/adapters)
#   3. export sql2pyspark:v10 (Q8_0) and sql2pyspark:v10-q4 (Q4_K_M; latest is v9-q4, so v10-q4 is the comparison)
#   4. evaluate v10-q4 on the 4 eval sets, the same 100-query complex_v8 test sample and the 50 held-out IN / NOT IN
#      pairs (v9-q4 too on those 50), then restart Docker + the app
# Logs: eval/start_training_v10.log (this script), sql2pyspark-mlx/classic_v10/train_v10.log.
# Promotion (make promote TAG=v10-q4 PREV=v9-q4) and the server are left to the owner.
cd /Users/praponb/GitHub/Finetuning-LLM
PY=sql2pyspark-mlx/.venv/bin/python
VENV=/Users/praponb/GitHub/Finetuning-LLM/sql2pyspark-mlx/.venv/bin
BASE=mlx-community/Qwen2.5-Coder-7B-Instruct-4bit
LOG=sql2pyspark-mlx/classic_v10/train_v10.log
D=sql2pyspark-mlx/classic_v10/data_mix
step() { echo "[$(date +%H:%M:%S)] $*"; }
unload() { for m in $(ollama ps | awk 'NR>1{print $1}'); do ollama stop $m; done }

[ -s $D/train.jsonl ] && [ -s $D/valid.jsonl ] || { step "classic_v10/data_mix missing; stopping"; exit 1; }
[ -s sql2pyspark-mlx/complex_v9/adapters/adapters.safetensors ] || { step "v9 adapters missing; stopping"; exit 1; }
[ -x llama.cpp/build/bin/llama-quantize ] || { step "llama-quantize missing; stopping"; exit 1; }
step "data ok: $(wc -l < $D/train.jsonl) train / $(wc -l < $D/valid.jsonl) valid"
if docker info >/dev/null 2>&1; then
  step "stopping Docker"; docker compose down; osascript -e 'quit app "Docker Desktop"'
  until ! pgrep -f "com.docker.virtualization|Docker Desktop.app/Contents/MacOS" >/dev/null; do sleep 2; done
fi
unload

if ! grep -q "^exit=0" $LOG 2>/dev/null; then
  step "start training"
  (echo "start $(date "+%Y-%m-%d %H:%M:%S %z")"
   cd sql2pyspark-mlx && .venv/bin/python train_resumable.py --config classic_v10/lora_config.yaml
   echo "exit=$?") > $LOG 2>&1
fi
grep -q "^exit=0" $LOG || { step "training failed; stopping"; exit 1; }
[ -s sql2pyspark-mlx/classic_v10/adapters/adapters.safetensors ] || { step "no v10 adapters; stopping"; exit 1; }

step "export v10 (fuse -> dequantized HF -> f16 GGUF -> Q8_0 + Q4_K_M -> Ollama)"
rm -rf sql2pyspark-hf
(cd sql2pyspark-mlx && $VENV/mlx_lm.fuse --model $BASE --adapter-path classic_v10/adapters --save-path ../sql2pyspark-hf --dequantize) \
  || { step "fuse failed"; exit 1; }
$PY llama.cpp/convert_hf_to_gguf.py sql2pyspark-hf --outtype f16 --outfile sql2pyspark-f16.gguf || { step "gguf failed"; exit 1; }
# the v9 files stay available as Ollama tags; the files on disk become v10
llama.cpp/build/bin/llama-quantize sql2pyspark-f16.gguf sql2pyspark.gguf Q8_0 >/dev/null 2>&1 || { step "Q8_0 failed"; exit 1; }
llama.cpp/build/bin/llama-quantize sql2pyspark-f16.gguf sql2pyspark-q4.gguf Q4_K_M >/dev/null 2>&1 || { step "Q4_K_M failed"; exit 1; }
rm -f sql2pyspark-f16.gguf
ollama create sql2pyspark:v10 -f Modelfile || { step "ollama create v10 failed"; exit 1; }
sed 's#^FROM .*#FROM ./sql2pyspark-q4.gguf#' Modelfile > Modelfile.q4
ollama create sql2pyspark:v10-q4 -f Modelfile.q4 || { step "ollama create v10-q4 failed"; exit 1; }
rm -f Modelfile.q4

M=v10-q4
step "$M holdout";      $PY eval/evaluate.py --model sql2pyspark:$M --set eval/holdout.jsonl --timeout 600
step "$M holdout_v4";   $PY eval/evaluate.py --model sql2pyspark:$M --set eval/holdout_v4.jsonl --timeout 600
step "$M real_queries"; $PY eval/evaluate.py --model sql2pyspark:$M --set eval/real_queries.jsonl --tables eval/real_tables.py --timeout 600
step "$M test_fixed";   $PY eval/evaluate.py --model sql2pyspark:$M --set eval/test_fixed.jsonl --timeout 600
step "$M complex_v8 test (100 of 300)"
$PY eval/evaluate.py --model sql2pyspark:$M --set eval/complex_v8_test.jsonl \
  --tables sql2pyspark-mlx/complex_v8/eval_tables.py --sample 100 --seed 0 --timeout 600
for T in v10-q4 v9-q4; do
  step "$T IN / NOT IN held-out pairs (50)"
  $PY eval/evaluate.py --model sql2pyspark:$T --set sql2pyspark-mlx/classic_v10/data/inlist_valid_meta.jsonl --timeout 600 \
    --out eval/results/inlist/${T}__inlist_valid.json
done
unload
step "restart Docker + app"; open -a "Docker Desktop"; until docker info >/dev/null 2>&1; do sleep 5; done
docker compose up -d app
step "ALL DONE"

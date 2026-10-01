#!/bin/zsh
# v9 launcher + post-training chain (one script). v9 = v8 continued on complex_v9/data_mix ("mirror" window code).
#   1. check the data, stop Docker and Ollama models (16 GB M2)
#   2. rebuild sql2pyspark:v8 in Ollama from complex_v8/adapters if it is missing (the Mac's Ollama store was empty)
#   3. train (complex_v9/lora_config.yaml, resumes complex_v8/adapters, writes complex_v9/adapters)
#   4. export sql2pyspark:v9 (Q8_0, as v6-v8) and sql2pyspark:v9-q4 (Q4_K_M: smaller/faster for the CPU-only server)
#   5. evaluate v9 on the 4 eval sets + the same 100-query complex_v8 test sample as v8, then v9-q4 on the same sets
# Logs: eval/start_training_v9.log (this script), sql2pyspark-mlx/complex_v9/train_v9.log.
# Promotion (make promote TAG=v9 PREV=v8) and the server are left to the owner.
cd /Users/praponb/GitHub/Finetuning-LLM
PY=sql2pyspark-mlx/.venv/bin/python
VENV=/Users/praponb/GitHub/Finetuning-LLM/sql2pyspark-mlx/.venv/bin
BASE=mlx-community/Qwen2.5-Coder-7B-Instruct-4bit
LOG=sql2pyspark-mlx/complex_v9/train_v9.log
D=sql2pyspark-mlx/complex_v9/data_mix
step() { echo "[$(date +%H:%M:%S)] $*"; }
unload() { for m in $(ollama ps | awk 'NR>1{print $1}'); do ollama stop $m; done }

# fuse <adapter dir> -> dequantized HF copy -> GGUF of the given type
to_gguf() {  # $1 adapters (relative to sql2pyspark-mlx/), $2 outtype, $3 outfile
  rm -rf sql2pyspark-hf
  (cd sql2pyspark-mlx && $VENV/mlx_lm.fuse --model $BASE --adapter-path $1 --save-path ../sql2pyspark-hf --dequantize) || return 1
  $PY llama.cpp/convert_hf_to_gguf.py sql2pyspark-hf --outtype $2 --outfile $3 || return 1
}

[ -s $D/train.jsonl ] && [ -s $D/valid.jsonl ] || { step "complex_v9/data_mix missing; stopping"; exit 1; }
[ -s sql2pyspark-mlx/complex_v8/adapters/adapters.safetensors ] || { step "v8 adapters missing; stopping"; exit 1; }
[ -x llama.cpp/build/bin/llama-quantize ] || { step "llama-quantize missing; stopping"; exit 1; }
step "data ok: $(wc -l < $D/train.jsonl) train / $(wc -l < $D/valid.jsonl) valid"
if docker info >/dev/null 2>&1; then
  step "stopping Docker"; docker compose down; osascript -e 'quit app "Docker Desktop"'
  until ! pgrep -f "com.docker.virtualization|Docker Desktop.app/Contents/MacOS" >/dev/null; do sleep 2; done
fi
unload

if ! ollama show sql2pyspark:v8 >/dev/null 2>&1; then
  step "rebuild sql2pyspark:v8 from complex_v8/adapters"
  to_gguf complex_v8/adapters q8_0 sql2pyspark.gguf && ollama create sql2pyspark:v8 -f Modelfile \
    || { step "v8 rebuild failed; stopping"; exit 1; }
fi

if ! grep -q "^exit=0" $LOG 2>/dev/null; then
  step "start training"
  (echo "start $(date "+%Y-%m-%d %H:%M:%S %z")"
   cd sql2pyspark-mlx && .venv/bin/python train_resumable.py --config complex_v9/lora_config.yaml
   echo "exit=$?") > $LOG 2>&1
fi
grep -q "^exit=0" $LOG || { step "training failed; stopping"; exit 1; }
[ -s sql2pyspark-mlx/complex_v9/adapters/adapters.safetensors ] || { step "no v9 adapters; stopping"; exit 1; }

step "export v9 (fuse -> dequantized HF -> f16 GGUF -> Q8_0 + Q4_K_M -> Ollama)"
to_gguf complex_v9/adapters f16 sql2pyspark-f16.gguf || { step "fuse/gguf failed"; exit 1; }
llama.cpp/build/bin/llama-quantize sql2pyspark-f16.gguf sql2pyspark.gguf Q8_0 >/dev/null 2>&1 || { step "Q8_0 failed"; exit 1; }
llama.cpp/build/bin/llama-quantize sql2pyspark-f16.gguf sql2pyspark-q4.gguf Q4_K_M >/dev/null 2>&1 || { step "Q4_K_M failed"; exit 1; }
rm -f sql2pyspark-f16.gguf
ollama create sql2pyspark:v9 -f Modelfile || { step "ollama create v9 failed"; exit 1; }
sed 's#^FROM .*#FROM ./sql2pyspark-q4.gguf#' Modelfile > Modelfile.q4
ollama create sql2pyspark:v9-q4 -f Modelfile.q4 || { step "ollama create v9-q4 failed"; exit 1; }
rm -f Modelfile.q4

for M in v9 v9-q4; do
  step "$M holdout";      $PY eval/evaluate.py --model sql2pyspark:$M --set eval/holdout.jsonl --timeout 600
  step "$M holdout_v4";   $PY eval/evaluate.py --model sql2pyspark:$M --set eval/holdout_v4.jsonl --timeout 600
  step "$M real_queries"; $PY eval/evaluate.py --model sql2pyspark:$M --set eval/real_queries.jsonl --tables eval/real_tables.py --timeout 600
  step "$M test_fixed";   $PY eval/evaluate.py --model sql2pyspark:$M --set eval/test_fixed.jsonl --timeout 600
  step "$M complex_v8 test (100 of 300)"
  $PY eval/evaluate.py --model sql2pyspark:$M --set sql2pyspark-mlx/complex_v8/data/test_meta.jsonl \
    --tables sql2pyspark-mlx/complex_v8/eval_tables.py --sample 100 --seed 0 --timeout 600 \
    --out eval/results/${M}__complex_v8_test.json
  unload
done
step "ALL DONE"

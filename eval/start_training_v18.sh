#!/bin/zsh
# v18 launcher + post-training chain (Mac mini M6). Two candidates, both scored on every eval set:
#   v18avg = exact average of the v15 and v17 adapters (sql2pyspark-mlx/v18/average.py, rank 32, no training)
#   v18a   = "gentle v18": v15 continued on v17's data (complex_v17/data_mix) at lr 2e-5 for 2,400 micro-batches
#            (sql2pyspark-mlx/v18/lora_config.yaml)
# Steps: 1. build v18avg  2. train v18a  3. export both (Q8_0 + Q4_K_M -> Ollama tags v18avg[-q4], v18a[-q4])
#        4. evaluate v18avg-q4 then v18a-q4 on the same sets as v17 (incl. the seeded 150 complex-shift pairs)
#        5. name slips  6. restore the root GGUF files to v15 / v15-q4 (live model) from the Ollama blobs
# Logs: eval/start_training_v18.log (this script), sql2pyspark-mlx/v18/train_v18a.log.
# Docker is not touched (not used on the mini). Promotion is left to the owner.
cd /Users/praponb/GitHub/Finetuning-LLM
PY=sql2pyspark-mlx/.venv/bin/python
VENV=/Users/praponb/GitHub/Finetuning-LLM/sql2pyspark-mlx/.venv/bin
BASE=mlx-community/Qwen2.5-Coder-7B-Instruct-4bit
LOG=sql2pyspark-mlx/v18/train_v18a.log
D=sql2pyspark-mlx/complex_v17/data_mix
H=sql2pyspark-mlx/complex_v16/data/complex_shift_meta.jsonl
HS=sql2pyspark-mlx/schema_v15/data/schema_shift_meta.jsonl
H12=sql2pyspark-mlx/targeted_v12/data/heldout_meta.jsonl
H11=sql2pyspark-mlx/targeted_v11/data/heldout_meta.jsonl
H13=sql2pyspark-mlx/targeted_v13/data/heldout_meta.jsonl
H14=sql2pyspark-mlx/targeted_v14/data/heldout_meta.jsonl
step() { echo "[$(date +%H:%M:%S)] $*"; }
unload() { for m in $(ollama ps 2>/dev/null | awk 'NR>1{print $1}'); do ollama stop $m; done }
ensure_ollama() { until ollama list >/dev/null 2>&1; do sleep 2; done }

[ -s $D/train.jsonl ] && [ -s $D/valid.jsonl ] && [ -s $H ] || { step "data missing; stopping"; exit 1; }
[ -s sql2pyspark-mlx/schema_v15/adapters/adapters.safetensors ] || { step "v15 adapters missing; stopping"; exit 1; }
[ -s sql2pyspark-mlx/complex_v17/adapters/adapters.safetensors ] || { step "v17 adapters missing; stopping"; exit 1; }
[ -x llama.cpp/build/bin/llama-quantize ] || { step "llama-quantize missing; stopping"; exit 1; }
caffeinate -s -i -w $$ &
step "data ok: $(wc -l < $D/train.jsonl) train / $(wc -l < $D/valid.jsonl) valid / $(wc -l < $H) held out"

# 1. v18avg (minutes)
if [ ! -s sql2pyspark-mlx/v18/adapters_avg/adapters.safetensors ]; then
  step "build v18avg (v15 + v17 average)"
  (cd sql2pyspark-mlx && .venv/bin/python v18/average.py) || { step "average failed; stopping"; exit 1; }
fi

# 2. v18a training
unload
if ! grep -q "^exit=0" $LOG 2>/dev/null; then
  step "start training v18a"
  (echo "start $(date "+%Y-%m-%d %H:%M:%S %z")"
   cd sql2pyspark-mlx && .venv/bin/python train_resumable.py --config v18/lora_config.yaml
   echo "exit=$?"; echo "end $(date "+%Y-%m-%d %H:%M:%S %z")") >> $LOG 2>&1
fi
grep -q "^exit=0" $LOG || { step "training failed; stopping"; exit 1; }
[ -s sql2pyspark-mlx/v18/adapters_a/adapters.safetensors ] || { step "no v18a adapters; stopping"; exit 1; }

# 3. export (the root GGUF files are overwritten here and restored to v15 in step 6)
export_tag() {   # $1 = tag, $2 = adapter dir (relative to sql2pyspark-mlx)
  ollama list | grep -q "^sql2pyspark:$1-q4 " && { step "$1-q4 already in Ollama"; return 0; }
  step "export $1 (fuse -> dequantized HF -> f16 GGUF -> Q8_0 + Q4_K_M -> Ollama)"
  rm -rf sql2pyspark-hf
  (cd sql2pyspark-mlx && $VENV/mlx_lm.fuse --model $BASE --adapter-path $2 --save-path ../sql2pyspark-hf --dequantize) \
    || { step "fuse $1 failed"; return 1; }
  $PY llama.cpp/convert_hf_to_gguf.py sql2pyspark-hf --outtype f16 --outfile sql2pyspark-f16.gguf || { step "gguf $1 failed"; return 1; }
  llama.cpp/build/bin/llama-quantize sql2pyspark-f16.gguf sql2pyspark.gguf Q8_0 >/dev/null 2>&1 || { step "Q8_0 $1 failed"; return 1; }
  llama.cpp/build/bin/llama-quantize sql2pyspark-f16.gguf sql2pyspark-q4.gguf Q4_K_M >/dev/null 2>&1 || { step "Q4_K_M $1 failed"; return 1; }
  rm -f sql2pyspark-f16.gguf
  ollama create sql2pyspark:$1 -f Modelfile || { step "ollama create $1 failed"; return 1; }
  sed 's#^FROM .*#FROM ./sql2pyspark-q4.gguf#' Modelfile > Modelfile.q4
  ollama create sql2pyspark:$1-q4 -f Modelfile.q4 || { step "ollama create $1-q4 failed"; return 1; }
  rm -f Modelfile.q4
}
ensure_ollama
export_tag v18avg v18/adapters_avg || exit 1
export_tag v18a v18/adapters_a || exit 1
rm -rf sql2pyspark-hf

# 4. evals
run_evals() {
  M=$1
  step "$M holdout";      $PY eval/evaluate.py --model sql2pyspark:$M --set eval/holdout.jsonl --timeout 600
  step "$M holdout_v4";   $PY eval/evaluate.py --model sql2pyspark:$M --set eval/holdout_v4.jsonl --timeout 600
  step "$M real_queries"; $PY eval/evaluate.py --model sql2pyspark:$M --set eval/real_queries.jsonl --tables eval/real_tables.py --timeout 600
  step "$M test_fixed";   $PY eval/evaluate.py --model sql2pyspark:$M --set eval/test_fixed.jsonl --timeout 600
  step "$M complex_v8 test (100 of 300)"
  $PY eval/evaluate.py --model sql2pyspark:$M --set eval/complex_v8_test.jsonl \
    --tables sql2pyspark-mlx/complex_v8/eval_tables.py --sample 100 --seed 0 --timeout 600
  step "$M IN / NOT IN held-out pairs (50)"
  $PY eval/evaluate.py --model sql2pyspark:$M --set sql2pyspark-mlx/classic_v10/data/inlist_valid_meta.jsonl --timeout 600 \
    --out eval/results/inlist/${M}__inlist_valid.json
  step "$M targeted_v11 held-out pairs (300)"
  $PY eval/evaluate.py --model sql2pyspark:$M --set $H11 --timeout 600 --out eval/results/v11-heldout/${M}__heldout.json
  step "$M targeted_v12 held-out pairs (250)"
  $PY eval/evaluate.py --model sql2pyspark:$M --set $H12 --timeout 600 --out eval/results/v12-heldout/${M}__heldout.json
  step "$M targeted_v13 held-out pairs (150)"
  $PY eval/evaluate.py --model sql2pyspark:$M --set $H13 --timeout 600 --out eval/results/v13-heldout/${M}__heldout.json
  step "$M targeted_v14 held-out pairs (200)"
  $PY eval/evaluate.py --model sql2pyspark:$M --set $H14 --timeout 600 --out eval/results/v14-heldout/${M}__heldout.json
  step "$M schema-shift pairs (200, v15 set)"
  $PY eval/evaluate.py --model sql2pyspark:$M --set $HS --tables eval/schema_shift_tables.py --timeout 600 \
    --out eval/results/schema-shift/${M}__schema_shift.json
  step "$M complex schema-shift pairs (150 of 300, unseen domains)"
  $PY eval/evaluate.py --model sql2pyspark:$M --set $H --sample 150 --seed 0 --tables eval/complex_shift_tables.py --timeout 600 \
    --out eval/results/complex-shift/${M}__complex_shift.json
  ollama stop sql2pyspark:$M 2>/dev/null
}
run_evals v18avg-q4
run_evals v18a-q4

# 5. name slips
step "name slips"; python3 eval/name_slips.py --models v15-q4 v17-q4 v18avg-q4 v18a-q4

# 6. root GGUF files back to the live model (v15 / v15-q4)
blob() { ollama show --modelfile sql2pyspark:$1 | awk '/^FROM \//{print $2; exit}'; }
B8=$(blob v15); B4=$(blob v15-q4)
if [ -s "$B8" ] && [ -s "$B4" ]; then
  cp "$B8" sql2pyspark.gguf && cp "$B4" sql2pyspark-q4.gguf && step "root GGUFs restored to v15 / v15-q4"
else
  step "WARNING: v15 blobs not found; root GGUFs are still v18a"
fi
step "ALL DONE"

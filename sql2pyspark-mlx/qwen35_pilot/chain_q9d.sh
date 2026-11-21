#!/bin/zsh
# Qwen3.5-9B round "q9d" on the Mac mini M6 (owner 2026-11-21, choice 3): final polish; resumes the q9c adapter.
#   1. train lora_config_q9d.yaml (2,400 micro-batches on data_q9d from adapters_q9c, auto-resume after Metal aborts,
#      new batch order per segment) with a divergence guard (stops if the latest val loss > 3x the best and > 0.05)
#   2. export: fuse --dequantize -> fix_norms -> f16 GGUF (--no-mtp) -> Q4_K_M -> Ollama sql2pyspark:q9d-q4
#   3. the same 12 eval sets as v18a / q9c (default result paths), 4. name slips vs v18a-q4 and q9c-q4 (real-style skipped)
# Never touches sql2pyspark:latest / v18a-q4 / v15-q4, the root GGUF files or the site. Promotion is the owner's call.
# Logs: qwen35_pilot/chain_q9d.log (run it with >> that file), qwen35_pilot/train_q9d.log. Resumable: rerun to continue.
cd /Users/praponb/GitHub/Finetuning-LLM
export PATH=/opt/homebrew/opt/openjdk@17/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin JAVA_HOME=/opt/homebrew/opt/openjdk@17
export HF_HUB_OFFLINE=1
M=sql2pyspark-mlx; P=$M/qwen35_pilot; PY=$M/.venv/bin/python; BASE=mlx-community/Qwen3.5-9B-4bit
T=q9d-q4; LOG=$P/train_q9d.log
H=$M/complex_v16/data/complex_shift_meta.jsonl
HS=$M/schema_v15/data/schema_shift_meta.jsonl
step() { echo "[$(date +%H:%M:%S)] $*"; }
unload() { for m in $(ollama ps 2>/dev/null | awk 'NR>1{print $1}'); do ollama stop $m; done }
ensure_ollama() { until ollama list >/dev/null 2>&1; do sleep 2; done }

# divergence guard: every 5 min compare the latest "Val loss" with the best so far
guard() {
  while kill -0 $1 2>/dev/null; do
    sleep 300
    local vals=$(grep -oE "Val loss [0-9.]+" $LOG | awk '{print $3}')
    [ $(echo "$vals" | grep -c .) -ge 3 ] || continue
    if echo "$vals" | awk 'NR==1{m=$1} {if($1<m)m=$1; l=$1} END{exit !(l > 3*m && l > 0.05)}'; then
      echo "DIVERGED $(date "+%F %T %z"): latest val loss > 3x best" >> $LOG
      pkill -f "train_resumable.py --config qwen35_pilot/lora_config_q9d.yaml"; sleep 2; pkill -f "qwen35_pilot/lora_q35.py"
      return
    fi
  done
}

[ -s $P/data_q9d/train.jsonl ] && [ -s $P/data_q9d/valid.jsonl ] || { step "data missing; stopping"; exit 1; }
[ -x llama.cpp/build/bin/llama-quantize ] || { step "llama-quantize missing; stopping"; exit 1; }
caffeinate -s -i -w $$ &
mkdir -p $P/gguf

# 1. training (the site's model is unloaded; it reloads on the next site request)
if ! grep -q "^exit=0" $LOG 2>/dev/null; then
  grep -q "^DIVERGED" $LOG 2>/dev/null && { step "training diverged earlier; stopping (see $LOG)"; exit 1; }
  unload; step "training q9d: 2,400 micro-batches from adapters_q9c on data_q9d (lr 3e-6 -> 3e-7)"
  (echo "start $(date "+%F %T %z")"
   cd $M && .venv/bin/python train_resumable.py --config qwen35_pilot/lora_config_q9d.yaml --launcher qwen35_pilot/lora_q35.py --max-restarts 40 --segment-val-batches 100
   echo "exit=$?"; echo "end $(date "+%F %T %z")") >> $LOG 2>&1 &
  TP=$!; guard $TP & GP=$!
  wait $TP; kill $GP 2>/dev/null
fi
grep -q "^DIVERGED" $LOG && { step "training DIVERGED; stopping"; exit 1; }
grep -q "^exit=0" $LOG || { step "training failed; stopping"; exit 1; }
step "training done: $(grep -E 'Val loss' $LOG | tail -1)"

# 2. export
if [ ! -s $P/gguf/q9d-q4.gguf ]; then
  unload; step "export: fuse (dequantized) -> fix_norms -> f16 GGUF -> Q4_K_M"
  rm -rf $P/hf_q9d
  (cd $M && .venv/bin/mlx_lm.fuse --model $BASE --adapter-path qwen35_pilot/adapters_q9d --save-path qwen35_pilot/hf_q9d --dequantize) \
    || { step "fuse failed"; exit 1; }
  $PY $P/fix_norms.py $P/hf_q9d || { step "fix_norms failed"; exit 1; }
  $PY llama.cpp/convert_hf_to_gguf.py $P/hf_q9d --outtype f16 --no-mtp --outfile $P/gguf/q9d-f16.gguf || { step "gguf failed"; exit 1; }
  llama.cpp/build/bin/llama-quantize $P/gguf/q9d-f16.gguf $P/gguf/q9d-q4.gguf Q4_K_M > $P/quantize_q9d.log 2>&1 || { step "quantize failed"; exit 1; }
  rm -rf $P/hf_q9d $P/gguf/q9d-f16.gguf
fi
ensure_ollama
if ! ollama list | grep -q "^sql2pyspark:$T "; then
  sed 's#^FROM .*#FROM ./gguf/q9d-q4.gguf#' $P/Modelfile.q35 > $P/Modelfile.q9d
  (cd $P && ollama create sql2pyspark:$T -f Modelfile.q9d) || { step "ollama create failed"; exit 1; }
fi

# 3. the 12 eval sets (default output paths, as for v18a / q35moe)
E() { $PY eval/evaluate.py --model sql2pyspark:$T --think false --timeout 600 "$@"; }
step "$T holdout";      E --set eval/holdout.jsonl
step "$T holdout_v4";   E --set eval/holdout_v4.jsonl
step "$T real_queries"; E --set eval/real_queries.jsonl --tables eval/real_tables.py
step "$T test_fixed";   E --set eval/test_fixed.jsonl
step "$T complex_v8 test (100 of 300)"
E --set eval/complex_v8_test.jsonl --tables $M/complex_v8/eval_tables.py --sample 100 --seed 0
step "$T IN / NOT IN held-out pairs (50)"
E --set $M/classic_v10/data/inlist_valid_meta.jsonl --out eval/results/inlist/${T}__inlist_valid.json
for v in 11 12 13 14; do
  step "$T targeted_v$v held-out pairs"
  E --set $M/targeted_v$v/data/heldout_meta.jsonl --out eval/results/v$v-heldout/${T}__heldout.json
done
step "$T schema-shift pairs (200, v15 set)"
E --set $HS --tables eval/schema_shift_tables.py --out eval/results/schema-shift/${T}__schema_shift.json
step "$T complex schema-shift pairs (150 of 300, unseen domains)"
E --set $H --sample 150 --seed 0 --tables eval/complex_shift_tables.py --out eval/results/complex-shift/${T}__complex_shift.json

# 4. name slips
step "name slips"; python3 eval/name_slips.py --models v18a-q4 q9c-q4 $T
unload

step "ALL DONE"

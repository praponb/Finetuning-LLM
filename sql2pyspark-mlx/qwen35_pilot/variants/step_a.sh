#!/bin/zsh
# Step A of the 11-23 improvement plan: precision checks, no training.
#   q9e: re-fuse -> f16 -> Q8_0 (q9e-q8), Q6_K (q9e-q6), imatrix Q4_K_M (q9e-q4im); q9d: Q8_0 (q9d-q8)
#   each: 3-query smoke test, then hold-out / v4 / fixed (+ dev set) -> eval/results/variants/
# Waits for chain_q9e.sh to finish. Resumable (every step skips what exists). Log: qwen35_pilot/variants/step_a.log
cd /Users/praponb/GitHub/Finetuning-LLM
source sql2pyspark-mlx/qwen35_pilot/variants/export_lib.sh
while pgrep -f "chain_q9e.sh" > /dev/null; do sleep 60; done
caffeinate -s -i -w $$ &
unload

step "q9e: fuse -> f16"
fuse_adapter qwen35_pilot/adapters_q9e $W/hf_q9e || { step "fuse q9e failed; stopping"; exit 1; }
to_f16 $W/hf_q9e $W/q9e-f16.gguf || { step "f16 q9e failed; stopping"; exit 1; }
rm -rf $W/hf_q9e

step "q9e: Q8_0 / Q6_K"
quant $W/q9e-f16.gguf $G/q9e-q8.gguf Q8_0 || { step "quant q8 failed; stopping"; exit 1; }
quant $W/q9e-f16.gguf $G/q9e-q6.gguf Q6_K || { step "quant q6 failed; stopping"; exit 1; }

step "q9e: imatrix (300 train pairs) -> Q4_K_M"
[ -s $V/calib_q9e.txt ] || $PY $V/make_calib.py 300 > $V/calib_q9e.txt
TAGS=(q9e-q8 q9e-q6 q9e-q4im q9d-q8)
if [ -s $V/q9e.imatrix ] || llama.cpp/build/bin/llama-imatrix -m $W/q9e-f16.gguf -f $V/calib_q9e.txt -o $V/q9e.imatrix \
     -c 1024 -ngl 99 > $V/imatrix_q9e.log 2>&1; then
  quant $W/q9e-f16.gguf $G/q9e-q4im.gguf Q4_K_M $V/q9e.imatrix || { step "quant q4im failed; stopping"; exit 1; }
else
  step "imatrix failed (see variants/imatrix_q9e.log); skipping q9e-q4im"; rm -f $V/q9e.imatrix; TAGS=(q9e-q8 q9e-q6 q9d-q8)
fi

step "q9d: fuse -> f16 -> Q8_0"
fuse_adapter qwen35_pilot/adapters_q9d $W/hf_q9d || { step "fuse q9d failed; stopping"; exit 1; }
to_f16 $W/hf_q9d $W/q9d-f16.gguf || { step "f16 q9d failed; stopping"; exit 1; }
rm -rf $W/hf_q9d
quant $W/q9d-f16.gguf $G/q9d-q8.gguf Q8_0 || { step "quant q9d q8 failed; stopping"; exit 1; }

for t in $TAGS; do
  tag $G/$t.gguf $t || { step "ollama create $t failed; stopping"; exit 1; }
  smoke $t || { step "smoke $t failed; stopping"; exit 1; }
  quick_evals $t
done
for t in q9e-q4 q9d-q4 v18a-q4; do dev_eval $t; done   # dev set baselines (their other results exist)
unload
rm -f $W/q9e-f16.gguf $W/q9d-f16.gguf   # my working files
step "STEP A DONE"

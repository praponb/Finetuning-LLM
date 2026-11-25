#!/bin/zsh
# Step B of the 11-23 improvement plan: weight averaging, no training.
#   q9e-avg: average of q9e checkpoints 15,800 / 16,400 / 16,600 / 16,800 (the ones kept; train_resumable keeps the
#            newest checkpoint of each restart segment)
#   q9de:    50 / 50 average of fused q9d and fused q9e
# Usage: zsh step_b.sh <precision: q4 | q4im | q6 | q8>   (Gate A's choice). Waits for step_a.sh.
# Tags sql2pyspark:q9e-avg-<prec>, q9de-<prec>; hold-out / v4 / fixed / dev -> eval/results/variants/. Resumable.
cd /Users/praponb/GitHub/Finetuning-LLM
source sql2pyspark-mlx/qwen35_pilot/variants/export_lib.sh
PREC=${1:?precision}
case $PREC in q4) QT=Q4_K_M; IM= ;; q4im) QT=Q4_K_M; IM=$V/q9e.imatrix ;; q6) QT=Q6_K; IM= ;; q8) QT=Q8_0; IM= ;;
  *) echo "bad precision"; exit 1 ;; esac
while pgrep -f "variants/step_a.sh" > /dev/null; do sleep 60; done
caffeinate -s -i -w $$ &
unload
A=qwen35_pilot/adapters_q9e

step "fuse q9e checkpoints 15800 / 16400 / 16600 / 16800 and q9d"
fuse_ckpt $A 0015800_adapters.safetensors $W/hf_c15800 || { step "fuse 15800 failed; stopping"; exit 1; }
fuse_ckpt $A 0016400_adapters.safetensors $W/hf_c16400 || { step "fuse 16400 failed; stopping"; exit 1; }
fuse_ckpt adapters_q9e_seg26 0000200_adapters.safetensors $W/hf_c16600 || { step "fuse 16600 failed; stopping"; exit 1; }
fuse_adapter $A $W/hf_q9e || { step "fuse q9e failed; stopping"; exit 1; }
fuse_adapter qwen35_pilot/adapters_q9d $W/hf_q9d || { step "fuse q9d failed; stopping"; exit 1; }
[ -s $W/hf_c15800/config.json ] && [ -s $W/hf_q9e/config.json ] || { step "fused folders missing; stopping"; exit 1; }

step "self-test of avg_weights.py"
$PY $V/avg_weights.py --selftest $W/hf_q9e || { step "selftest failed; stopping"; exit 1; }

for name in q9e-avg q9de; do
  t=$name-$PREC
  if [ ! -s $G/$t.gguf ]; then
    step "$t: average -> f16 -> $QT"
    rm -rf $W/hf_$name
    if [ $name = q9e-avg ]; then
      $PY $V/avg_weights.py $W/hf_$name $W/hf_c15800 $W/hf_c16400 $W/hf_c16600 $W/hf_q9e || { step "avg failed; stopping"; exit 1; }
    else
      $PY $V/avg_weights.py $W/hf_$name $W/hf_q9d $W/hf_q9e || { step "avg failed; stopping"; exit 1; }
    fi
    to_f16 $W/hf_$name $W/$name-f16.gguf || { step "f16 $name failed; stopping"; exit 1; }
    rm -rf $W/hf_$name
    quant $W/$name-f16.gguf $G/$t.gguf $QT $IM || { step "quant $t failed; stopping"; exit 1; }
    rm -f $W/$name-f16.gguf
  fi
  tag $G/$t.gguf $t || { step "ollama create $t failed; stopping"; exit 1; }
  smoke $t || { step "smoke $t failed; stopping"; exit 1; }
  quick_evals $t
done
rm -rf $W/hf_c15800 $W/hf_c16400 $W/hf_c16600 $W/hf_q9e $W/hf_q9d   # my working folders
step "STEP B DONE"

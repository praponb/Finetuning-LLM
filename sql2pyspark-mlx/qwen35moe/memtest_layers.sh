#!/bin/zsh
# Round 3 prep (user 2026-11-16 ~09:40: "try LoRA on more layers"): memory + speed for more LoRA layers on Qwen3.5-35B-A3B.
# 12 micro-batches (3 optimizer updates) on the 64 longest samples, full keys, rank 16, for each (seq, layers) below;
# a config that crashes with an out-of-memory error is reported as FAIL and the next one runs.
# Log: qwen35moe/memtest_layers.log. Run from the repo root. The site's model is unloaded first.
cd /Users/praponb/GitHub/Finetuning-LLM
M=sql2pyspark-mlx; D=$M/qwen35moe
export HF_HUB_OFFLINE=1
step() { echo "[$(date +%H:%M:%S)] $*"; }
for m in $(ollama ps 2>/dev/null | awk 'NR>1{print $1}'); do ollama stop $m; done
caffeinate -s -i -w $$ &
for cfg in "2600 24" "2600 32" "2600 40" "2300 40" "2300 32"; do
  set -- ${=cfg}; s=$1; l=$2
  y=$D/memtest_${s}_${l}_full.yaml; log=$D/memtest_${s}_${l}_full.log
  sed -e "s/^max_seq_length:.*/max_seq_length: $s/" -e "s/^num_layers:.*/num_layers: $l/" \
      -e "s/^iters:.*/iters: 12/" -e "s/^steps_per_report:.*/steps_per_report: 4/" $D/memtest.yaml > $y
  step "memtest seq $s, $l layers"
  (cd $M && .venv/bin/python qwen35moe/lora_moe.py -c qwen35moe/$(basename $y)) > $log 2>&1
  rc=$?
  peak=$(grep -oE "Peak mem [0-9.]+ GB" $log | tail -1); tps=$(grep -oE "Tokens/sec [0-9.]+" $log | tail -1)
  if [ $rc -eq 0 ] && grep -q "Iter 12:" $log; then step "PASS seq $s / $l layers: $peak, $tps"
  else step "FAIL seq $s / $l layers (rc $rc, $peak): $(grep -iE 'error|out of memory' $log | tail -1 | cut -c1-160)"; fi
done
rm -rf $D/adapters_memtest
step "MEMTEST DONE"

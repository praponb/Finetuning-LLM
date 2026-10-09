#!/bin/zsh
# Resume pilot step 3 from iteration 150 (the first attempt hit a Metal OOM after 150 and train_resumable.py's
# segment-1 SameFileError bug stopped the automatic resume), then continue the chain (export, evals).
cd /Users/praponb/GitHub/Finetuning-LLM/sql2pyspark-mlx
caffeinate -s -i -w $$ &
(echo "resume from 150 $(date "+%F %T %z")"
 HF_HUB_OFFLINE=1 .venv/bin/python train_resumable.py --config qwen35_pilot/resume150.yaml --launcher qwen35_pilot/lora_q35.py --max-restarts 20
 rc=$?
 [ $rc = 0 ] && cp qwen35_pilot/adapters_resume150/adapters.safetensors qwen35_pilot/adapters/adapters.safetensors
 echo "exit=$rc"; echo "end $(date "+%F %T %z")") >> qwen35_pilot/train.log 2>&1
cd .. && exec zsh sql2pyspark-mlx/qwen35_pilot/chain.sh

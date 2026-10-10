"""mlx_lm.lora for Qwen3.5 on the 16 GB M2: chunked (WY-form) Gated DeltaNet in training mode (gd_chunked.py) and a
chunked lm_head + masked cross-entropy loss (chunked_loss.py, also used for validation). Same arguments as
mlx_lm.lora; batch_size must be 1.

MLX's default Metal command buffers are used: train_resumable.py sets MLX_MAX_OPS_PER_BUFFER=2 /
MLX_MAX_MB_PER_BUFFER=40 for the Qwen2.5 runs, which made Qwen3.5 steps trip the GPU watchdog more often
(set Q35_SMALL_BUFFERS=1 to keep them). gd_patch.py (kernel forward + per-token recompute backward) is the earlier
attempt: memory-bounded, but compiled steps tripped the watchdog and it was ~25% slower.

Usage (from sql2pyspark-mlx/):  .venv/bin/python qwen35_pilot/lora_q35.py -c qwen35_pilot/lora_config.yaml
"""
import functools
import os
import sys
from pathlib import Path

if not os.environ.get("Q35_SMALL_BUFFERS"):
    os.environ.pop("MLX_MAX_OPS_PER_BUFFER", None)
    os.environ.pop("MLX_MAX_MB_PER_BUFFER", None)
sys.path.insert(0, str(Path(__file__).resolve().parent))
import gd_chunked  # noqa: E402
from chunked_loss import chunked_loss  # noqa: E402

gd_chunked.install()
import mlx.core as mx  # noqa: E402
import mlx_lm.lora as lora  # noqa: E402

# MLX memory: active memory peaks at ~12.25 GB (2,600-token samples). With a 1 GB free-buffer cache on top a step ran
# out of Metal memory (recommended working set on the 16 GB M2: ~12.7 GB); with no cache at all the first step after a
# resume tripped the GPU watchdog twice (allocation churn). So: a 512 MB cache and a 12 GB memory limit (MLX frees the
# cache and waits for running work before it goes past it).
mx.set_cache_limit(512 << 20)
mx.set_memory_limit(12 << 30)

lora.train = functools.partial(lora.train, loss=chunked_loss)
lora.main()

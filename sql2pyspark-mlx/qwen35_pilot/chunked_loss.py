"""Memory-bounded loss for Qwen3.5 (248k vocabulary) LoRA training in mlx-lm 0.31.3 (pilot only).

mlx-lm's default_loss materialises the logits of every position: at 3,200 tokens that is 3,200 x 248,320 values
(1.6 GB in bf16, plus the softmax and the gradient of the same size). This loss runs the decoder once and applies
lm_head + masked cross-entropy in checkpointed chunks of CHUNK positions, so at most one chunk of logits exists at a
time. Same value as default_loss (sum over the target tokens / their number). No .item() / .tolist(): mlx-lm compiles
the training step.
"""
from __future__ import annotations

import mlx.core as mx
import mlx.nn as nn

CHUNK = 256


def chunked_loss(model, batch, lengths):
    inputs = batch[:, :-1]
    targets = batch[:, 1:]
    lm = model.language_model
    head = lm.model.embed_tokens.as_linear if lm.args.tie_word_embeddings else lm.lm_head

    h = lm.model(inputs)
    steps = mx.arange(1, targets.shape[1] + 1)
    mask = mx.logical_and(steps >= lengths[:, 0:1], steps <= lengths[:, 1:])   # as default_loss

    def piece(t_c, m_c):   # targets / mask are closed over: checkpoint differentiates every argument (no VJP for indices)
        return mx.checkpoint(lambda h_c: (nn.losses.cross_entropy(head(h_c), t_c) * m_c).astype(mx.float32).sum())

    total = mx.array(0.0)
    for c in range(0, targets.shape[1], CHUNK):
        total = total + piece(targets[:, c:c + CHUNK], mask[:, c:c + CHUNK])(h[:, c:c + CHUNK])
    ntoks = mask.sum()
    return total / ntoks, ntoks

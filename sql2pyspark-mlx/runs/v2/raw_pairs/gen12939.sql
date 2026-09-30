SELECT id, method, amount, DENSE_RANK() OVER (PARTITION BY method ORDER BY amount DESC) AS amount_dense_rank FROM payments WHERE paid_at >= '2024-07-01';

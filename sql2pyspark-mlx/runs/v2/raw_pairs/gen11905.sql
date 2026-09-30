SELECT id, order_id, amount, DENSE_RANK() OVER (PARTITION BY order_id ORDER BY amount) AS amount_dense_rank FROM payments WHERE status != 'FAILED'

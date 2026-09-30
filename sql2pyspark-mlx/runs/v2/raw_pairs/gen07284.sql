SELECT id, channel, amount, DENSE_RANK() OVER (PARTITION BY channel ORDER BY amount DESC) AS amount_dense_rank FROM orders

SELECT id, amount, DENSE_RANK() OVER (ORDER BY amount) AS amount_dense_rank FROM payments ORDER BY amount DESC, amount_dense_rank DESC;

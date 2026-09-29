SELECT id, method, amount, COUNT(*) OVER (PARTITION BY method) AS method_count, DENSE_RANK() OVER (PARTITION BY method ORDER BY amount DESC) AS amount_dense_rank FROM payments;

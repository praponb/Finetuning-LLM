SELECT
    id,
    method,
    amount,
    DENSE_RANK() OVER (PARTITION BY method ORDER BY amount) amount_dense_rank
FROM payments

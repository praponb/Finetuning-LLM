SELECT
    id,
    status,
    amount,
    DENSE_RANK() OVER (PARTITION BY status ORDER BY amount) AS amount_dense_rank
FROM payments;

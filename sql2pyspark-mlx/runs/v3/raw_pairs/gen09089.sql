SELECT
    id,
    status,
    paid_at,
    DENSE_RANK() OVER (PARTITION BY status ORDER BY paid_at) AS paid_at_dense_rank
FROM payments

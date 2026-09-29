SELECT
    id,
    method,
    paid_at,
    DENSE_RANK() OVER (PARTITION BY method ORDER BY paid_at) AS paid_at_dense_rank
FROM payments
WHERE status IN ('FAILED', 'REFUNDED')

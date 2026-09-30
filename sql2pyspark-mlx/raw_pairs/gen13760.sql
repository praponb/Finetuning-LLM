SELECT id, paid_at, DENSE_RANK() OVER (ORDER BY paid_at DESC) AS paid_at_dense_rank
FROM payments
ORDER BY paid_at

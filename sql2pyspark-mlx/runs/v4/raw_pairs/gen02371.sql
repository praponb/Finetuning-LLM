SELECT id, paid_at, DENSE_RANK() OVER (ORDER BY paid_at) paid_at_dense_rank
FROM payments
WHERE amount > 1050;

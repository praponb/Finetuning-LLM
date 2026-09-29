SELECT
    id,
    order_id,
    paid_at,
    DENSE_RANK() OVER (PARTITION BY order_id ORDER BY paid_at) paid_at_dense_rank
FROM payments
ORDER BY 2 DESC, paid_at_dense_rank

SELECT
    id,
    order_id,
    paid_at,
    RANK() OVER (PARTITION BY order_id ORDER BY paid_at) AS paid_at_rank
FROM payments
ORDER BY order_id;

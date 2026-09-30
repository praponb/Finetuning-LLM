SELECT
    id,
    method,
    paid_at,
    order_id,
    FIRST_VALUE(order_id) OVER (PARTITION BY method ORDER BY paid_at, id) AS first_order_id
FROM payments
ORDER BY id DESC;

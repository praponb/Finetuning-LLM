SELECT
    id,
    order_id,
    paid_at,
    status,
    FIRST_VALUE(status) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS first_status
FROM payments

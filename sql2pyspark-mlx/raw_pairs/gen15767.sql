SELECT
    id,
    method,
    order_id,
    CAST(paid_at AS DATE) AS paid_at_day
FROM payments
WHERE order_id IS NOT NULL

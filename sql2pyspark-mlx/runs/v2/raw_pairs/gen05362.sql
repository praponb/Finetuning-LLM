SELECT
    id,
    paid_at,
    order_id,
    LENGTH(status) AS status_length,
    HOUR(paid_at) AS paid_at_hour
FROM payments
WHERE paid_at < '2025-01-24'

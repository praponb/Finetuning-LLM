SELECT
    id,
    amount,
    paid_at,
    status,
    HOUR(paid_at) AS paid_at_hour
FROM payments
WHERE order_id = 4
ORDER BY amount DESC

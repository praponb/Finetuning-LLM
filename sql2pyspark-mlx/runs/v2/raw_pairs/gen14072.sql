SELECT method
FROM payments
WHERE status = 'REFUNDED'
UNION ALL
SELECT method
FROM payments
WHERE order_id IS NULL

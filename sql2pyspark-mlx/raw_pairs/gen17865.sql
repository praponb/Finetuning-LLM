SELECT method, order_id
FROM payments
WHERE order_id IS NOT NULL
UNION
SELECT method, order_id
FROM payments
WHERE status = 'FAILED'

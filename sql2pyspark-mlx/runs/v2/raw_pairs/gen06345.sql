SELECT status
FROM payments
WHERE order_id IN (5, 20, 34, 37)
UNION
SELECT status
FROM shipments

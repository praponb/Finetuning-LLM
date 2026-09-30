SELECT status
FROM orders
UNION ALL
SELECT status
FROM shipments
WHERE order_id = 40
ORDER BY status DESC

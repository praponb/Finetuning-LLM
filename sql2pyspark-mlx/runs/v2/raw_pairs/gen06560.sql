SELECT status
FROM orders
WHERE status = 'SHIPPED'
UNION ALL
SELECT status
FROM shipments

SELECT status
FROM orders
UNION ALL
SELECT status
FROM shipments
ORDER BY status;

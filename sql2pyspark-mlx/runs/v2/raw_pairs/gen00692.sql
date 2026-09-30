SELECT status
FROM orders
UNION ALL
SELECT status
FROM shipments
WHERE order_id IS NOT NULL;

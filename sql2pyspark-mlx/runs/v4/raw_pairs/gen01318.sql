SELECT order_id
FROM shipments
WHERE order_id IN (11, 13, 27)
UNION
SELECT order_id
FROM shipments
WHERE order_id IS NOT NULL

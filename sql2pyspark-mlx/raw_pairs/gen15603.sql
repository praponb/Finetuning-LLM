SELECT status, order_id
FROM shipments
WHERE order_id IN (3, 10, 30, 31)
UNION
SELECT status, order_id
FROM shipments
WHERE cost >= 46

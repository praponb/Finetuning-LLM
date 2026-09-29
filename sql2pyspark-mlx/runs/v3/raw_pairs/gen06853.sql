SELECT status
FROM payments
WHERE order_id = 14
INTERSECT
SELECT status
FROM shipments;

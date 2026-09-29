SELECT carrier, order_id
FROM shipments
INTERSECT
SELECT carrier, order_id
FROM shipments
WHERE status IN ('RETURNED', 'LOST', 'DELIVERED');

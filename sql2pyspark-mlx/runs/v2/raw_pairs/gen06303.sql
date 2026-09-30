SELECT s.id, s.status, s.carrier
FROM shipments s
WHERE s.order_id IS NULL AND s.order_id IN (SELECT id FROM orders WHERE customer_id IN (5, 33))
ORDER BY status, id
LIMIT 5

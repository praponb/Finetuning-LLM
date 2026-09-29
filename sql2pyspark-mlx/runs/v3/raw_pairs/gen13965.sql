SELECT s.id, s.carrier
FROM shipments s
WHERE s.order_id NOT IN (SELECT id FROM orders WHERE status IN ('PENDING', 'RETURNED', 'COMPLETED'))
ORDER BY s.carrier ASC, id ASC
LIMIT 10

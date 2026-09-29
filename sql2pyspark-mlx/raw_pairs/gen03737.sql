SELECT s.id, s.status, s.cost
FROM shipments s
WHERE s.order_id NOT IN (SELECT id FROM orders WHERE status IN ('CANCELLED', 'RETURNED'));

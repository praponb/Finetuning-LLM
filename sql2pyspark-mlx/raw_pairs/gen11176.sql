SELECT s.id, s.cost, s.carrier FROM shipments s WHERE s.order_id NOT IN (SELECT id FROM orders WHERE status = 'CANCELLED')

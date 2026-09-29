SELECT s.id, s.status, s.carrier FROM shipments s WHERE s.status NOT IN ('RETURNED', 'IN_TRANSIT') AND s.order_id IN (SELECT id FROM orders WHERE status = 'COMPLETED');

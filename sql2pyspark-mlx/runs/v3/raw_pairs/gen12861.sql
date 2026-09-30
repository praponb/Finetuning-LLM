SELECT s.id, s.carrier FROM shipments s WHERE s.order_id IN (SELECT id FROM orders WHERE status NOT IN ('COMPLETED', 'RETURNED', 'SHIPPED'));

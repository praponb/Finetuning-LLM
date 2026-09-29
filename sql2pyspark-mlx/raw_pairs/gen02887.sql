SELECT o.id, o.amount, o.status FROM orders o WHERE EXISTS (SELECT 1 FROM shipments s WHERE s.order_id = o.id);

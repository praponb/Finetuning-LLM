SELECT o.id, o.amount, o.order_date, o.status FROM orders o WHERE o.id IN (SELECT order_id FROM shipments WHERE status = 'DELIVERED') ORDER BY o.status, id LIMIT 50;

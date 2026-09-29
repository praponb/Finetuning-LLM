SELECT o.id, o.order_date FROM orders o WHERE o.customer_id = 8 AND NOT EXISTS (SELECT 1 FROM shipments s WHERE s.order_id = o.id);

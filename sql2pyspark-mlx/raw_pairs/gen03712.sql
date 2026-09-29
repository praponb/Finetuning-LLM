SELECT o.id, o.order_date FROM orders o WHERE NOT EXISTS (SELECT 1 FROM shipments s WHERE s.order_id = o.id) ORDER BY order_date

SELECT o.id, o.customer_id FROM orders o WHERE EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id AND oi.product_id IN (6, 18, 22));

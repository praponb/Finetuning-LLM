SELECT o.id, o.discount FROM orders o WHERE NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id AND oi.order_id = 29) ORDER BY o.id DESC, discount DESC;

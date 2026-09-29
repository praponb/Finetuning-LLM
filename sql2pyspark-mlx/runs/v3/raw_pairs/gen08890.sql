SELECT o.id, o.channel, o.customer_id FROM orders o WHERE NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id AND oi.quantity = 7) ORDER BY o.channel;

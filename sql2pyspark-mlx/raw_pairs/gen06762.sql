SELECT o.id, o.shipping_fee, o.channel FROM orders o WHERE o.store_id IS NULL AND NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id) ORDER BY o.channel;

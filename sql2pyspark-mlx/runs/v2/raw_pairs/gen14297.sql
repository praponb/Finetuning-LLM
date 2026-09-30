SELECT o.id, o.status, o.order_date, o.channel FROM orders o WHERE o.store_id IS NOT NULL AND EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id AND oi.product_id = 12);

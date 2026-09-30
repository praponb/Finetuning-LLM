SELECT o.id, o.channel, o.shipping_fee, o.order_date FROM orders o WHERE o.store_id IN (SELECT id FROM stores WHERE region = 'Central') ORDER BY order_date;

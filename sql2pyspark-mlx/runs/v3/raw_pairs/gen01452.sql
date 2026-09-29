SELECT o.id, o.channel, o.shipping_fee, o.customer_id FROM orders o WHERE o.store_id IN (SELECT id FROM stores WHERE city = 'Singapore') ORDER BY customer_id, id LIMIT 10;

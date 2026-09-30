SELECT o.id, o.order_date, o.channel, o.shipping_fee FROM orders o WHERE o.store_id IN (SELECT id FROM stores WHERE sqft <= 6066) ORDER BY id DESC;

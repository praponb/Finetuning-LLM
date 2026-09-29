SELECT o.id, o.channel, o.status FROM orders o WHERE o.customer_id IN (10, 11, 24, 32) AND o.store_id NOT IN (SELECT id FROM stores WHERE sqft <> 12455);

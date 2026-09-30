SELECT o.id, o.channel
FROM orders o
WHERE o.customer_id IS NOT NULL AND o.store_id NOT IN (SELECT id FROM stores WHERE sqft <> 8917);

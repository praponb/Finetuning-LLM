SELECT o.id, o.discount
FROM orders o
WHERE o.store_id NOT IN (SELECT id FROM stores WHERE sqft < 11561)

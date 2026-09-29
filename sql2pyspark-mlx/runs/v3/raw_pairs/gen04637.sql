SELECT o.id, o.discount FROM orders o WHERE o.store_id NOT IN (SELECT id FROM stores WHERE sqft BETWEEN 9358 AND 10975)

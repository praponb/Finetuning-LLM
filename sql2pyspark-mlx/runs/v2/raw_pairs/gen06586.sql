SELECT s.id, s.store_name FROM stores s WHERE s.sqft = 12441 AND NOT EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id)

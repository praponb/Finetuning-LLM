SELECT s.id, s.sqft FROM stores s WHERE EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id);

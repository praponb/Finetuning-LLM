SELECT s.id, s.region, s.sqft, s.store_name FROM stores s WHERE EXISTS (SELECT 1 FROM store_sales ss WHERE ss.store_id = s.id AND ss.store_id IN (1, 4))

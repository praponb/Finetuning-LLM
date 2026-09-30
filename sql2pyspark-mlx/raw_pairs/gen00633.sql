SELECT s.id, s.sqft, s.store_name, s.region FROM stores s WHERE EXISTS (SELECT 1 FROM store_sales ss WHERE ss.store_id = s.id);

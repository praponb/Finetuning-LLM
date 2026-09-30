SELECT s.id, s.region, s.sqft FROM stores s WHERE EXISTS (SELECT 1 FROM inventory i WHERE i.store_id = s.id);

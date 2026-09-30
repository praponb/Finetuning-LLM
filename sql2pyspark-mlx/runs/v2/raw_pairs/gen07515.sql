SELECT s.id, s.store_name, s.sqft, s.city FROM stores s WHERE s.sqft > 9068 AND NOT EXISTS (SELECT 1 FROM inventory i WHERE i.store_id = s.id);

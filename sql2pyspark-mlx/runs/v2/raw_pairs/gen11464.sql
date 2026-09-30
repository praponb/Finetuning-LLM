SELECT s.id, s.city, s.store_name FROM stores s WHERE s.store_name LIKE '%Flagshi%' AND EXISTS (SELECT 1 FROM inventory i WHERE i.store_id = s.id) ORDER BY id DESC;

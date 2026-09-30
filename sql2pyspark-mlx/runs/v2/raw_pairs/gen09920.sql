SELECT s.id, s.region, s.store_name, s.opened_date FROM stores s WHERE s.sqft = 5577 AND NOT EXISTS (SELECT 1 FROM inventory i WHERE i.store_id = s.id)

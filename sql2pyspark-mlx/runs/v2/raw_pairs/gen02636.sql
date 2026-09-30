SELECT s.id, s.sqft, s.store_name, s.region FROM stores s WHERE s.id IN (SELECT store_id FROM orders WHERE amount > 890)

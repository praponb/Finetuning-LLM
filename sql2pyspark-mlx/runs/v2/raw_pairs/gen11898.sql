SELECT s.id, s.sqft FROM stores s WHERE s.region = 'West' AND s.id IN (SELECT store_id FROM inventory) ORDER BY sqft DESC, id LIMIT 5

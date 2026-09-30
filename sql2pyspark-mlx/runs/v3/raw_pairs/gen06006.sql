SELECT s.id, s.region FROM stores s WHERE s.sqft IS NOT NULL AND s.id IN (SELECT store_id FROM inventory);

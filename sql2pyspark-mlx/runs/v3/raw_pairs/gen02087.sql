SELECT s.id, s.sqft
FROM stores s
WHERE s.store_name LIKE 'Ai%' AND s.id IN (SELECT store_id FROM inventory)

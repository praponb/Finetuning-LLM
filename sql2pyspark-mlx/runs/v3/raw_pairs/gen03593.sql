SELECT s.id, s.sqft, s.store_name
FROM stores s
WHERE s.region IN ('West', 'East') AND s.id IN (SELECT store_id FROM inventory WHERE product_id IN (5, 8));

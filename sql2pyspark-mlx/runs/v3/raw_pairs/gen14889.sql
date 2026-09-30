SELECT
    s.id,
    s.region,
    s.sqft,
    s.city
FROM stores s
WHERE s.id IN (SELECT store_id FROM inventory WHERE quantity < 85)
ORDER BY id
LIMIT 10;

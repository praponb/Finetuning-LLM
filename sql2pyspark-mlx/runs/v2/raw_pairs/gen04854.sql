SELECT s.id, s.region
FROM stores s
WHERE s.sqft = 14958 AND s.id IN (SELECT store_id FROM orders WHERE amount <= 670)
ORDER BY s.id, region

SELECT s.id, s.city, s.region
FROM stores s
WHERE s.sqft IS NULL AND NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.store_id = s.id AND ss.store_id IN (2, 5))
ORDER BY s.region;

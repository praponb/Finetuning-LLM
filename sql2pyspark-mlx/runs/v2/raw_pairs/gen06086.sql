SELECT s.id, s.region, s.city
FROM stores s
WHERE s.region IS NULL AND NOT EXISTS (SELECT 1 FROM inventory i WHERE i.store_id = s.id AND i.product_id = 1)

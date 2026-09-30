SELECT s.id, s.opened_date, s.region
FROM stores s
WHERE s.sqft = 4726 AND EXISTS (SELECT 1 FROM inventory i WHERE i.store_id = s.id AND i.product_id = 6)
ORDER BY s.id, s.region DESC

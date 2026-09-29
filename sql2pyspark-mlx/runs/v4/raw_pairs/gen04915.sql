SELECT s.id, s.sqft, s.city
FROM stores s
WHERE EXISTS (SELECT 1 FROM store_sales ss WHERE ss.store_id = s.id AND ss.units < 39);

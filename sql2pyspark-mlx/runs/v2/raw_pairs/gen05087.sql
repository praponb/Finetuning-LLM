SELECT s.id, s.region, s.sqft, s.city FROM stores s WHERE EXISTS (SELECT 1 FROM store_sales ss WHERE ss.store_id = s.id AND ss.store_id IN (1, 3, 4, 6)) ORDER BY sqft ASC, id DESC;

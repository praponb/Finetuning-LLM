SELECT s.id, s.sqft, s.opened_date FROM stores s WHERE NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.store_id = s.id AND ss.product_id IN (3, 4, 7, 8));

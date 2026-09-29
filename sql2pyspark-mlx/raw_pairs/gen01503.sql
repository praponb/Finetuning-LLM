SELECT s.id, s.city, s.opened_date FROM stores s WHERE NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.store_id = s.id AND ss.product_id IN (1, 2, 4)) ORDER BY s.opened_date;

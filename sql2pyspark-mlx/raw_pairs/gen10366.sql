SELECT s.id, s.store_name FROM stores s WHERE NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.store_id = s.id AND ss.revenue IS NULL)

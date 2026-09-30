SELECT s.store_name, ss.store_id FROM stores s LEFT JOIN store_sales ss ON s.id = ss.store_id;

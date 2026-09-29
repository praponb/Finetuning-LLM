SELECT s.region, COUNT(ss.id) store_sale_count FROM stores s LEFT JOIN store_sales ss ON ss.store_id = s.id WHERE ss.store_id IN (2, 5) GROUP BY s.region HAVING COUNT(ss.id) >= 5;

SELECT s.id, COUNT(ss.id) AS store_sale_count FROM stores s LEFT OUTER JOIN store_sales ss ON ss.store_id = s.id GROUP BY s.id ORDER BY store_sale_count DESC;

SELECT s.city, COUNT(ss.id) AS store_sale_count, MAX(ss.sale_date) AS last_sale_date FROM stores s LEFT OUTER JOIN store_sales ss ON ss.store_id = s.id GROUP BY s.city HAVING COUNT(ss.id) >= 5;

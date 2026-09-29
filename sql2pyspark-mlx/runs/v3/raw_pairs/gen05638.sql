SELECT ss.product_id, ss.sale_date, s.region, s.city, s.store_name FROM store_sales ss LEFT OUTER JOIN stores s ON ss.store_id = s.id WHERE LENGTH(s.store_name) >= 9 ORDER BY s.city DESC;

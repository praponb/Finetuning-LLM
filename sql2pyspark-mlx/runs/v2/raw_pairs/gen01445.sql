SELECT ss.product_id, ss.store_id, s.city, p.brand, LENGTH(s.region) region_length FROM store_sales ss LEFT JOIN stores s ON ss.store_id = s.id JOIN products p ON ss.product_id = p.id;

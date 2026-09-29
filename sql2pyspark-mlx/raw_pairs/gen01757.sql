SELECT ss.product_id, ss.store_id, s.region, p.cost FROM store_sales ss LEFT JOIN stores s ON ss.store_id = s.id LEFT JOIN products p ON p.id = ss.product_id WHERE p.brand = 'Stark';

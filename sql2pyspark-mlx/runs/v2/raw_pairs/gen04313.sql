SELECT ss.id, ss.units, ss.product_id, p.brand, p.product_name, s.store_name, s.sqft FROM store_sales ss LEFT JOIN products p ON p.id = ss.product_id INNER JOIN stores s ON ss.store_id = s.id

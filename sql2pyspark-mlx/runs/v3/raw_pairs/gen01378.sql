SELECT ss.units, p.supplier_id, s.city FROM store_sales ss INNER JOIN products p ON ss.product_id = p.id LEFT JOIN stores s ON ss.store_id = s.id ORDER BY ss.units, city;

SELECT ss.product_id, ss.units, s.sqft, s.city FROM store_sales ss LEFT JOIN stores s ON s.id = ss.store_id WHERE ss.revenue >= 3300;

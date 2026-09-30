SELECT ss.id, ss.units, s.sqft, s.opened_date FROM store_sales ss LEFT JOIN stores s ON ss.store_id = s.id ORDER BY s.sqft DESC;

SELECT ss.units, s.opened_date, s.sqft, s.city FROM store_sales ss LEFT JOIN stores s ON ss.store_id = s.id WHERE ss.store_id IN (1, 2, 4) AND ss.revenue >= 1500 ORDER BY sqft DESC, opened_date

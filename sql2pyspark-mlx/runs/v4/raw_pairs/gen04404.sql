SELECT ss.id, ss.store_id, s.opened_date FROM store_sales ss LEFT JOIN stores s ON s.id = ss.store_id WHERE s.store_name LIKE 'M%' AND s.sqft > 8426

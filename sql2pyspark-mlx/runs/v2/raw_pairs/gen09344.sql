SELECT ss.store_id, s.opened_date, s.sqft FROM store_sales ss INNER JOIN stores s ON ss.store_id = s.id WHERE s.sqft = 10963 AND ss.product_id IN (1, 5, 6, 8) ORDER BY ss.store_id DESC

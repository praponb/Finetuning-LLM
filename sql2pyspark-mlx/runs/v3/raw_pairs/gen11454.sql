SELECT ss.id, ss.product_id, s.opened_date, s.sqft FROM store_sales ss INNER JOIN stores s ON ss.store_id = s.id WHERE ss.product_id IN (4, 8) AND ss.product_id IN (1, 4)

SELECT ss.id, ss.product_id, s.region, s.opened_date, s.sqft FROM store_sales ss INNER JOIN stores s ON ss.store_id = s.id ORDER BY s.opened_date

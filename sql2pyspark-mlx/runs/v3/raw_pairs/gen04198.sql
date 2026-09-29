SELECT ss.id, ss.product_id, ss.revenue, ss.units, s.store_name, s.opened_date FROM store_sales ss INNER JOIN stores s ON s.id = ss.store_id WHERE s.sqft <> 10096

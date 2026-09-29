SELECT ss.id, ss.store_id, ss.sale_date, ss.product_id, s.store_name, s.opened_date, s.city FROM store_sales ss LEFT JOIN stores s ON s.id = ss.store_id WHERE ss.units IS NOT NULL

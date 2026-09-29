SELECT ss.id, ss.sale_date, s.opened_date, s.city, s.store_name FROM store_sales ss FULL OUTER JOIN stores s ON ss.store_id = s.id

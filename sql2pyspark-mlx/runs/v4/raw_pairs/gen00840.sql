SELECT s.opened_date, ss.store_id, ss.product_id FROM stores s LEFT OUTER JOIN store_sales ss ON ss.store_id = s.id WHERE MONTH(s.opened_date) IN (6, 7, 10) ORDER BY ss.store_id, s.opened_date

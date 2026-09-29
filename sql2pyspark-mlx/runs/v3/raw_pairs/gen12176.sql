SELECT s.region, s.city, s.store_name, ss.revenue, ss.sale_date, ss.product_id FROM stores s LEFT JOIN store_sales ss ON s.id = ss.store_id WHERE s.opened_date > '2023-12-03' ORDER BY city, 5

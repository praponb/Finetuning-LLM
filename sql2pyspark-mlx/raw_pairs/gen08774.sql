SELECT ss.id, ss.sale_date, ss.revenue, s.city, s.region FROM store_sales ss INNER JOIN stores s ON s.id = ss.store_id WHERE ss.revenue IS NOT NULL

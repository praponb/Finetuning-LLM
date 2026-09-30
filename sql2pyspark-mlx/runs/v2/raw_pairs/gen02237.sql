SELECT s.id, s.region FROM stores s WHERE s.id IN (SELECT store_id FROM store_sales WHERE MONTH(sale_date) IN (3, 7, 12)) ORDER BY s.region, id LIMIT 10

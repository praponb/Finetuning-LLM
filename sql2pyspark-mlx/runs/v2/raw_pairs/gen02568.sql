SELECT * FROM (SELECT store_id, product_id, COUNT(units) AS units_count FROM store_sales WHERE store_id IN (1, 4, 5, 6) GROUP BY store_id, product_id) AS sub WHERE units_count >= 5

SELECT product_id, MAX(units) AS max_units, AVG(units) AS avg_units FROM store_sales WHERE product_id IN (4, 7, 8, 10) AND revenue <= 1500 GROUP BY product_id HAVING AVG(units) > 13

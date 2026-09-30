SELECT store_id, AVG(units) AS avg_units, MAX(units) AS max_units FROM store_sales WHERE units BETWEEN 23 AND 33 AND product_id = 8 GROUP BY store_id ORDER BY max_units DESC, 1 DESC

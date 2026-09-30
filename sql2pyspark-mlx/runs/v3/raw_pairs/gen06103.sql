SELECT store_id, ROUND(AVG(units), 2) AS avg_units FROM store_sales GROUP BY store_id HAVING avg_units >= 37

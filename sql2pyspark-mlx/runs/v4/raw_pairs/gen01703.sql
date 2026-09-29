SELECT product_id, SUM(units) AS total_units
FROM store_sales
GROUP BY 1
HAVING total_units > 100

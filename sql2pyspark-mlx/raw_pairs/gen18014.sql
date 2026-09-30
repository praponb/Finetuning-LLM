SELECT product_id, ROUND(AVG(units), 2) AS avg_units, MAX(units) AS max_units FROM store_sales GROUP BY product_id;

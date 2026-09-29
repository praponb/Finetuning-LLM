SELECT product_id, AVG(units) AS avg_units
FROM store_sales
GROUP BY product_id

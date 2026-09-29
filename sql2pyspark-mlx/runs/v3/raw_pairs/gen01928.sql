SELECT product_id, SUM(units) AS total_units, RANK() OVER (ORDER BY SUM(units) DESC) AS rnk
FROM store_sales
GROUP BY product_id
ORDER BY 1 DESC

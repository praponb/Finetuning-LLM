SELECT product_id, SUM(units) AS total_units, SUM(SUM(units)) OVER (ORDER BY product_id) AS cumulative_units
FROM store_sales
WHERE sale_date BETWEEN '2024-03-01' AND '2024-04-01'
GROUP BY product_id
ORDER BY cumulative_units DESC, product_id
LIMIT 20

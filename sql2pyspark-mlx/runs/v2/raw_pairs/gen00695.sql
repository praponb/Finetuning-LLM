SELECT product_id, AVG(revenue) AS avg_revenue, COUNT(units) AS units_count
FROM store_sales
WHERE revenue BETWEEN 3100 AND 3200 AND store_id IN (1, 2)
GROUP BY product_id
HAVING units_count >= 3
ORDER BY avg_revenue DESC;

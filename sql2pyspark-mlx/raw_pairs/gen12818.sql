SELECT product_id, MIN(revenue) AS min_revenue, MAX(revenue) AS max_revenue
FROM store_sales
WHERE revenue >= 1600
GROUP BY product_id
ORDER BY product_id DESC
LIMIT 20

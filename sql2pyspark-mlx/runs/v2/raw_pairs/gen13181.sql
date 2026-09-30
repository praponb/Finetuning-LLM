SELECT product_id, AVG(revenue) avg_revenue, MIN(revenue) min_revenue
FROM store_sales
GROUP BY product_id
ORDER BY avg_revenue

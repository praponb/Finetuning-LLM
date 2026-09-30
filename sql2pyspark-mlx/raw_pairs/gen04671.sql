SELECT product_id, CASE WHEN revenue >= 3100 THEN 'high' WHEN revenue >= 700 THEN 'mid' ELSE 'low' END AS revenue_bucket, ROUND(AVG(revenue), 2) AS avg_revenue
FROM store_sales
GROUP BY CUBE(product_id, CASE WHEN revenue >= 3100 THEN 'high' WHEN revenue >= 700 THEN 'mid' ELSE 'low' END)
ORDER BY avg_revenue DESC

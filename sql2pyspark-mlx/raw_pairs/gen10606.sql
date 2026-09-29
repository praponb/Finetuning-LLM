SELECT
    store_id,
    CASE WHEN revenue >= 4500 THEN 'high' WHEN revenue >= 500 THEN 'mid' ELSE 'low' END AS revenue_bucket,
    product_id,
    ROUND(AVG(revenue), 2) AS avg_revenue
FROM store_sales
GROUP BY 1, 2, 3
ORDER BY revenue_bucket, avg_revenue DESC, store_id, product_id
LIMIT 20;

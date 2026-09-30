SELECT
    CASE WHEN revenue >= 3500 THEN 'high' WHEN revenue >= 600 THEN 'mid' ELSE 'low' END AS revenue_bucket,
    MAX(revenue) - MIN(revenue) AS revenue_range,
    SUM(revenue) AS total_revenue,
    COUNT(*) AS cnt
FROM store_sales
GROUP BY revenue_bucket
HAVING COUNT(*) > 2;

WITH filtered_store_sales AS (
    SELECT *
    FROM store_sales
    WHERE units <= 12 AND YEAR(sale_date) >= 2024
),
agg AS (
    SELECT CASE WHEN revenue >= 3900 THEN 'high' WHEN revenue >= 2000 THEN 'mid' ELSE 'low' END AS revenue_bucket, CASE WHEN units >= 38 THEN 'high' WHEN units >= 13 THEN 'mid' ELSE 'low' END AS units_bucket, SUM(revenue) AS total_revenue
    FROM filtered_store_sales
    GROUP BY CASE WHEN revenue >= 3900 THEN 'high' WHEN revenue >= 2000 THEN 'mid' ELSE 'low' END, CASE WHEN units >= 38 THEN 'high' WHEN units >= 13 THEN 'mid' ELSE 'low' END
)
SELECT revenue_bucket, units_bucket, total_revenue
FROM agg
WHERE total_revenue > 14000
ORDER BY revenue_bucket DESC;

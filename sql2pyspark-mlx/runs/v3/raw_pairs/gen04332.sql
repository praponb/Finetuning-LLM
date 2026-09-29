WITH src AS (
    SELECT *
    FROM store_sales
    WHERE revenue BETWEEN 1900 AND 3700
),
revenue_bucket_summary AS (
    SELECT CASE WHEN revenue >= 3700 THEN 'high' WHEN revenue >= 800 THEN 'mid' ELSE 'low' END revenue_bucket, SUM(revenue) total_revenue
    FROM src
    GROUP BY CASE WHEN revenue >= 3700 THEN 'high' WHEN revenue >= 800 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM revenue_bucket_summary
ORDER BY revenue_bucket
LIMIT 5;

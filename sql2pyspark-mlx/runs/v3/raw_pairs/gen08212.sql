WITH src AS (
    SELECT *
    FROM store_sales
    WHERE store_id IN (1, 3, 4, 5)
),
store_sales_stats AS (
    SELECT store_id, CASE WHEN units >= 38 THEN 'high' WHEN units >= 14 THEN 'mid' ELSE 'low' END AS units_bucket, ROUND(AVG(revenue), 2) AS avg_revenue
    FROM src
    GROUP BY store_id, CASE WHEN units >= 38 THEN 'high' WHEN units >= 14 THEN 'mid' ELSE 'low' END
)
SELECT store_id, units_bucket
FROM store_sales_stats
ORDER BY store_id;

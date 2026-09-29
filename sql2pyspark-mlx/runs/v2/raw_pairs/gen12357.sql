WITH Base AS (
    SELECT *
    FROM store_sales
    WHERE units BETWEEN 25 AND 34
),
revenue_bucket_summary AS (
    SELECT CASE WHEN revenue >= 4200 THEN 'high' WHEN revenue >= 1700 THEN 'mid' ELSE 'low' END AS revenue_bucket, store_id, MIN(sale_date) AS first_sale_date
    FROM Base
    GROUP BY CASE WHEN revenue >= 4200 THEN 'high' WHEN revenue >= 1700 THEN 'mid' ELSE 'low' END, store_id
)
SELECT *
FROM revenue_bucket_summary

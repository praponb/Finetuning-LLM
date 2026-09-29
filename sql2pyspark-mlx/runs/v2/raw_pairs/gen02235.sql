WITH src AS (
    SELECT *
    FROM store_sales
    WHERE YEAR(sale_date) >= 2023 AND product_id IN (5, 8)
),
store_sales_stats AS (
    SELECT CASE WHEN revenue >= 4000 THEN 'high' WHEN revenue >= 800 THEN 'mid' ELSE 'low' END AS revenue_bucket, SUM(units) AS total_units, AVG(revenue) AS avg_revenue
    FROM src
    GROUP BY CASE WHEN revenue >= 4000 THEN 'high' WHEN revenue >= 800 THEN 'mid' ELSE 'low' END
)
SELECT revenue_bucket, avg_revenue
FROM store_sales_stats
WHERE avg_revenue >= 1700
ORDER BY revenue_bucket;

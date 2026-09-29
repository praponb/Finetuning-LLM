WITH base AS (
    SELECT *
    FROM products
    WHERE launch_date < '2024-10-12' AND supplier_id IN (11, 16, 32) AND price <= 640
),
launch_cohort_summary AS (
    SELECT CASE WHEN YEAR(launch_date) < 2024 THEN 'before_2024' WHEN YEAR(launch_date) = 2024 THEN '2024' ELSE 'after_2024' END AS launch_cohort, MAX(stock_qty) - MIN(stock_qty) AS stock_qty_range
    FROM base
    GROUP BY CASE WHEN YEAR(launch_date) < 2024 THEN 'before_2024' WHEN YEAR(launch_date) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT *
FROM launch_cohort_summary
WHERE stock_qty_range >= 38
ORDER BY stock_qty_range DESC

SELECT
    CASE WHEN YEAR(launch_date) < 2024 THEN 'before_2024' WHEN YEAR(launch_date) = 2024 THEN '2024' ELSE 'after_2024' END AS launch_cohort,
    DATE_TRUNC('MONTH', launch_date) AS launch_month,
    MIN(price) AS min_price,
    COUNT(*) AS product_count,
    SUM(CASE WHEN category = 'Electronics' THEN 1 ELSE 0 END) AS electronics_count
FROM products
WHERE supplier_id IN (5, 8, 27)
GROUP BY launch_cohort, launch_month
HAVING COUNT(*) < 3
ORDER BY launch_month, min_price DESC, launch_cohort
LIMIT 5

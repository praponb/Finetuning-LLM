SELECT
    CASE WHEN YEAR(p.launch_date) < 2024 THEN 'before_2024' WHEN YEAR(p.launch_date) = 2024 THEN '2024' ELSE 'after_2024' END AS launch_cohort,
    QUARTER(p.launch_date) AS launch_quarter,
    MAX(ss.revenue) AS max_revenue,
    COUNT(ss.units) AS units_count
FROM store_sales ss
JOIN products p ON ss.product_id = p.id
GROUP BY CASE WHEN YEAR(p.launch_date) < 2024 THEN 'before_2024' WHEN YEAR(p.launch_date) = 2024 THEN '2024' ELSE 'after_2024' END, QUARTER(p.launch_date)

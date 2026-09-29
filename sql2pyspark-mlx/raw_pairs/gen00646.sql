SELECT CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END opened_cohort, ROUND((100 * SUM(CASE WHEN region = 'South' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_south
FROM stores
GROUP BY 1
ORDER BY opened_cohort

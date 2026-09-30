SELECT CASE WHEN YEAR(launch_date) < 2024 THEN 'before_2024' WHEN YEAR(launch_date) = 2024 THEN '2024' ELSE 'after_2024' END AS launch_cohort, SUM(cost) AS total_cost, ROUND(SUM(cost) * 100.0 / SUM(SUM(cost)) OVER (), 2) AS pct_of_total
FROM products
WHERE category IN ('Sports', 'Grocery')
GROUP BY CASE WHEN YEAR(launch_date) < 2024 THEN 'before_2024' WHEN YEAR(launch_date) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY total_cost, launch_cohort

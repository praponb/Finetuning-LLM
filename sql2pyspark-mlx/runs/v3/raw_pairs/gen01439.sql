SELECT CASE WHEN YEAR(launch_date) < 2024 THEN 'before_2024' WHEN YEAR(launch_date) = 2024 THEN '2024' ELSE 'after_2024' END AS launch_cohort, SUM(CASE WHEN category = 'Sports' THEN cost ELSE 0 END) AS sports_cost
FROM products
GROUP BY 1
ORDER BY launch_cohort;

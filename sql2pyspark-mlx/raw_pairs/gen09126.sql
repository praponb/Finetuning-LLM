SELECT YEAR(launch_date) AS launch_year, ROUND((100 * SUM(CASE WHEN category = 'Electronics' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_electronics
FROM products
WHERE launch_date >= '2024-08-19'
GROUP BY YEAR(launch_date)

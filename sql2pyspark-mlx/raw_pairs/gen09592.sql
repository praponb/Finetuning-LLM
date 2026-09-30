SELECT QUARTER(launch_date) AS launch_quarter, MAX(launch_date) AS latest_launch_date, ROUND((100 * SUM(CASE WHEN category = 'Fashion' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_fashion
FROM products
WHERE cost >= 560
GROUP BY launch_quarter
HAVING pct_fashion >= 25
ORDER BY latest_launch_date

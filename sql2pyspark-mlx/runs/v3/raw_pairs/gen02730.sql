SELECT category, MIN(launch_date) AS first_launch_date, SUM(CASE WHEN brand = 'Umbrella' THEN 1 ELSE 0 END) AS umbrella_count
FROM products
WHERE launch_date >= '2025-03-15'
GROUP BY category;

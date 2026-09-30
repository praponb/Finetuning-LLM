SELECT category, MIN(launch_date) min_launch_date
FROM products
GROUP BY category
ORDER BY 2, category
LIMIT 10

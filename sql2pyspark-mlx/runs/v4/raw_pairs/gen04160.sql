SELECT category, MIN(launch_date) AS first_launch_date FROM products GROUP BY category

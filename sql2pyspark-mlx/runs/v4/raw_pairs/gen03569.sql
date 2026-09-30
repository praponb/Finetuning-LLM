SELECT category, MIN(launch_date) AS min_launch_date FROM products GROUP BY category;

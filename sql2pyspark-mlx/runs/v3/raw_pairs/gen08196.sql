SELECT category, MAX(launch_date) AS latest_launch_date FROM products GROUP BY category;

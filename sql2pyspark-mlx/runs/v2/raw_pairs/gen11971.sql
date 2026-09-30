SELECT category, brand, MIN(launch_date) AS first_launch_date FROM products GROUP BY ROLLUP(category, brand) ORDER BY first_launch_date;

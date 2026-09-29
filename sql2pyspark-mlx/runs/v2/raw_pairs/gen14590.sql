SELECT YEAR(launch_date) AS launch_year, ROUND(STDDEV(cost), 2) AS stddev_cost, MIN(launch_date) AS first_launch_date FROM products GROUP BY YEAR(launch_date) ORDER BY launch_year LIMIT 3;

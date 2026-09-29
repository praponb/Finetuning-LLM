SELECT MONTH(launch_date) AS launch_month, MIN(launch_date) AS min_launch_date, COUNT(cost) AS cost_count FROM products GROUP BY 1 HAVING COUNT(cost) > 3

SELECT MONTH(launch_date) launch_month, MIN(launch_date) first_launch_date FROM products GROUP BY MONTH(launch_date)

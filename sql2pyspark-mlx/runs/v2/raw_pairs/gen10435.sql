SELECT DATE_TRUNC('MONTH', launch_date) AS launch_month, category, MIN(launch_date) AS first_launch_date FROM products GROUP BY DATE_TRUNC('MONTH', launch_date), category

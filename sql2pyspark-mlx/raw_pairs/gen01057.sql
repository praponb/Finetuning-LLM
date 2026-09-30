SELECT DATE_TRUNC('MONTH', launch_date) AS launch_month, brand, MAX(launch_date) AS last_launch_date FROM products GROUP BY DATE_TRUNC('MONTH', launch_date), brand;

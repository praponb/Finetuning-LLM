SELECT DATE_TRUNC('MONTH', launch_date) AS launch_month, MIN(launch_date) AS first_launch_date, SUM(CASE WHEN brand = 'Acme' THEN cost ELSE 0 END) AS acme_cost
FROM products
GROUP BY DATE_TRUNC('MONTH', launch_date);

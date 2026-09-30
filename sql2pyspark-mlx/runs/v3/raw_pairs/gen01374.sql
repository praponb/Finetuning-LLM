SELECT DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_month, SUM(cost) AS total_cost, COUNT(cost) AS cost_count FROM products GROUP BY DATE_FORMAT(launch_date, 'yyyy-MM');

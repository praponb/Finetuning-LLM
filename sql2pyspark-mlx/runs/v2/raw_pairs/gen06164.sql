SELECT brand, QUARTER(launch_date) AS launch_quarter, DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_month, ROUND(AVG(cost), 2) AS avg_cost FROM products GROUP BY 1, 2, 3;

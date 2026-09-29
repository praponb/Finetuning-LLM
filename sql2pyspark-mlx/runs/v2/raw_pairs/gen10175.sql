SELECT DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_month, AVG(price) AS avg_price FROM products GROUP BY DATE_FORMAT(launch_date, 'yyyy-MM') ORDER BY launch_month;

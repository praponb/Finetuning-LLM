SELECT DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_month, MAX(cost) AS max_cost, MAX(launch_date) AS latest_launch_date
FROM products
GROUP BY launch_month
HAVING max_cost >= 310;

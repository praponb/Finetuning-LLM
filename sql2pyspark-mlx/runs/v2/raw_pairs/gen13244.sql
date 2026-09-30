SELECT YEAR(launch_date) AS launch_year, SUM(cost) AS total_cost FROM products GROUP BY YEAR(launch_date);

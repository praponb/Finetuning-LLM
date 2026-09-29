SELECT MONTH(launch_date) AS launch_month, MIN(launch_date) AS first_launch_date FROM products WHERE cost >= 340 GROUP BY MONTH(launch_date) ORDER BY first_launch_date, launch_month LIMIT 3;

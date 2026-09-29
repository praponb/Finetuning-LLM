SELECT brand, MIN(launch_date) AS first_launch_date FROM products GROUP BY brand HAVING COUNT(price) > 5;

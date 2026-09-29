SELECT brand, category, MIN(launch_date) AS min_launch_date, MIN(price) AS min_price FROM products GROUP BY brand, category ORDER BY brand;

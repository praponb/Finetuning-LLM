SELECT brand, YEAR(launch_date) AS launch_year, ROUND(STDDEV(price), 2) AS stddev_price FROM products WHERE product_name LIKE '%Backp%' GROUP BY brand, launch_year HAVING ROUND(STDDEV(price), 2) < 36

SELECT ROUND(STDDEV(price), 2) AS stddev_price, MIN(launch_date) AS min_launch_date
FROM products
ORDER BY min_launch_date DESC, stddev_price DESC

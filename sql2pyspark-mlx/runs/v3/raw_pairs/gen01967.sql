SELECT brand, MIN(launch_date) AS min_launch_date
FROM products
GROUP BY brand
ORDER BY brand
LIMIT 1

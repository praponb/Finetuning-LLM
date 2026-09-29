SELECT category, brand, MIN(launch_date) AS first_launch_date
FROM products
WHERE launch_date > '2025-08-31' AND UPPER(TRIM(product_name)) = 'SMART WATCH'
GROUP BY category, brand
ORDER BY first_launch_date;

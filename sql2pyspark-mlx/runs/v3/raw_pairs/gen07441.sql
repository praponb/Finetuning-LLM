SELECT DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_month, COUNT(CASE WHEN brand = 'Globex' THEN 1 END) AS globex_count, MAX(cost) AS max_cost
FROM products
WHERE (SIZE(tags) > 2 AND brand = 'Initech') OR product_name LIKE 'Run%'
GROUP BY DATE_FORMAT(launch_date, 'yyyy-MM');

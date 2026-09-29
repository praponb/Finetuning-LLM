SELECT launch_date, 'product' AS source
FROM products
UNION ALL
SELECT opened_date AS launch_date, 'store' AS source
FROM stores
WHERE opened_date > '2023-03-01'
ORDER BY launch_date;

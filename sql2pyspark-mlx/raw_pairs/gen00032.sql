SELECT DISTINCT brand, category
FROM products
WHERE MONTH(launch_date) IN (4, 7, 11)
ORDER BY category DESC

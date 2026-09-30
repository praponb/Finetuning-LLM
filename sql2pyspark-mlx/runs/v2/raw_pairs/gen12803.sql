SELECT launch_date, category
FROM products
WHERE cost > 380
EXCEPT
SELECT launch_date, category
FROM products
WHERE YEAR(launch_date) >= 2024

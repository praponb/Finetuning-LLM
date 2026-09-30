SELECT category, SUM(CASE WHEN category = 'Books' THEN price ELSE 0 END) AS books_price
FROM products
WHERE YEAR(launch_date) = 2024
GROUP BY category
HAVING COUNT(CASE WHEN brand = 'Globex' THEN 1 END) >= 5;

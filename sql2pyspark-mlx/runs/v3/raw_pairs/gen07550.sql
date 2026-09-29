SELECT launch_date, category
FROM products
WHERE price > 700
UNION ALL
SELECT launch_date, category
FROM products
WHERE cost >= 640
ORDER BY category DESC;

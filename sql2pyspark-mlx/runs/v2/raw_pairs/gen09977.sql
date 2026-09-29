SELECT category, COUNT(*) AS total_count
FROM products
GROUP BY category
HAVING COUNT(price) >= 3
ORDER BY category

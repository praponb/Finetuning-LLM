SELECT category, AVG(price) avg_price, ROUND((100 * SUM(CASE WHEN category = 'Sports' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_sports
FROM products
WHERE category = 'Sports' AND price BETWEEN 360 AND 970 AND brand IS NULL
GROUP BY 1
ORDER BY avg_price DESC, category
LIMIT 50;

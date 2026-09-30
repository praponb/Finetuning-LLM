SELECT category, MAX(price) - MIN(price) AS price_range, COUNT(DISTINCT price) AS unique_categorys
FROM products
GROUP BY category
HAVING MAX(price) - MIN(price) < 25;

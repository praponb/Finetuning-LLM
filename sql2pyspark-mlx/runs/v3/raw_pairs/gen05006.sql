SELECT category, brand, MAX(price) - MIN(price) AS price_range
FROM products
GROUP BY 1, 2
ORDER BY price_range DESC

SELECT
    id,
    category,
    price,
    AVG(price) OVER (PARTITION BY category) AS category_avg_price
FROM products
ORDER BY price

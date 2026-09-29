SELECT
    id,
    category,
    price,
    RANK() OVER (PARTITION BY category ORDER BY price DESC) AS rnk
FROM products
WHERE SIZE(tags) > 1

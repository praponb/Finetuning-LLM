SELECT
    id,
    tags,
    brand,
    ROUND(price * 0.85, 2) AS price_adjusted
FROM products
WHERE cost >= 440
ORDER BY price_adjusted DESC, brand DESC;

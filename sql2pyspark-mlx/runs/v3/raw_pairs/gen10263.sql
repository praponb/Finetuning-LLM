SELECT
    id,
    category,
    price,
    AVG(price) OVER (PARTITION BY category) category_avg_price,
    RANK() OVER (PARTITION BY category ORDER BY price) price_rank
FROM products
ORDER BY category DESC;

SELECT
    id,
    category,
    price,
    brand,
    RANK() OVER (PARTITION BY category ORDER BY price) AS price_rank,
    ROUND(price / SUM(price) OVER (PARTITION BY brand), 3) AS price_share
FROM products;

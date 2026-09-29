SELECT
    id,
    brand,
    price,
    PERCENT_RANK() OVER (PARTITION BY brand ORDER BY price) AS price_pct_rank
FROM products
WHERE category = 'Home'
ORDER BY price_pct_rank DESC

SELECT
    id,
    supplier_id,
    price,
    ROUND(price / SUM(price) OVER (PARTITION BY supplier_id), 3) AS price_share
FROM products

SELECT
    launch_date,
    stock_qty,
    brand,
    ARRAY_JOIN(tags, '|') AS tags_text,
    CAST(price AS INT) AS price_int
FROM products
WHERE stock_qty BETWEEN 113 AND 131

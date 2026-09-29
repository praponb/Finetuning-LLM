SELECT
    id,
    price,
    stock_qty,
    tags,
    SIZE(tags) AS tags_count
FROM products

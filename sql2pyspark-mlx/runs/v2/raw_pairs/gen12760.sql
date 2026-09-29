SELECT
    id,
    tags,
    category,
    price + cost margin,
    FLOOR(stock_qty) stock_qty_whole
FROM products
WHERE price IS NOT NULL
ORDER BY id DESC
LIMIT 10;

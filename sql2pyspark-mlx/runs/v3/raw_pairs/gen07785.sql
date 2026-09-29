SELECT
    id,
    tags,
    stock_qty,
    SORT_ARRAY(tags) AS sorted_tags
FROM products
ORDER BY stock_qty DESC;

SELECT
    id,
    stock_qty,
    category,
    price,
    TRIM(category) AS clean_category
FROM products
WHERE ARRAY_CONTAINS(tags, 'eco');

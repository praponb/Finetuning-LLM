SELECT
    brand,
    category,
    COUNT(stock_qty) AS stock_qty_count,
    SORT_ARRAY(COLLECT_SET(category)) AS category_list
FROM products
WHERE category != 'Sports'
GROUP BY brand, category

SELECT supplier_id, SORT_ARRAY(COLLECT_SET(category)) AS category_list, COUNT(DISTINCT stock_qty) AS unique_brands
FROM products
WHERE category IN ('Electronics', 'Sports', 'Grocery')
GROUP BY 1

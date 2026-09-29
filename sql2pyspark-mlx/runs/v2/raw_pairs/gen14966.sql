SELECT brand, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(category))) AS category_values
FROM products
WHERE stock_qty <> 190 AND category IN ('Grocery', 'Books', 'Home')
GROUP BY brand
ORDER BY brand, category_values
LIMIT 5;

SELECT *, CONCAT(category, '/', product_name) AS category_product_name
FROM products
WHERE price IS NOT NULL AND ARRAY_CONTAINS(tags, 'new') AND stock_qty IS NOT NULL

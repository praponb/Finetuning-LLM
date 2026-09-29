SELECT *, CONCAT(category, ', ', brand) category_brand, cost * price margin
FROM products
WHERE supplier_id IS NOT NULL OR ARRAY_CONTAINS(tags, 'sale')
ORDER BY product_name, id
LIMIT 5

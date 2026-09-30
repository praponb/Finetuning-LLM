SELECT *, ARRAY_CONTAINS(tags, 'premium') AS is_premium
FROM products
WHERE supplier_id IS NOT NULL
ORDER BY product_name DESC, id
LIMIT 10;

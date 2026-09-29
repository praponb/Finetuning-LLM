SELECT id, brand
FROM products
WHERE category <> 'Fashion'
ORDER BY id DESC, brand
LIMIT 5

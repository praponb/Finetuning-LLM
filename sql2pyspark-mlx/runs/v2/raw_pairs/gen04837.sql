SELECT id, brand
FROM products
WHERE price < 440 AND cost IS NOT NULL AND stock_qty <= 216
ORDER BY brand, id
LIMIT 5

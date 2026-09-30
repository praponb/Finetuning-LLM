SELECT p.id, p.product_name
FROM products p
WHERE p.id IN (SELECT product_id FROM inventory)
ORDER BY id DESC
LIMIT 50

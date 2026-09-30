SELECT id, quantity
FROM inventory
WHERE store_id = 2
UNION
SELECT id, quantity
FROM inventory
WHERE product_id IN (2, 4, 6, 8)
ORDER BY id DESC

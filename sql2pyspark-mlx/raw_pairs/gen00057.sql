SELECT id, quantity
FROM inventory
WHERE product_id IN (5, 9)
UNION ALL
SELECT id, quantity
FROM inventory
WHERE product_id IN (3, 4);

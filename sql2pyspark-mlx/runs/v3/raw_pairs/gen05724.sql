SELECT id, quantity
FROM inventory
WHERE product_id = 6
UNION ALL
SELECT id, quantity
FROM inventory
WHERE last_restock > '2025-06-01';

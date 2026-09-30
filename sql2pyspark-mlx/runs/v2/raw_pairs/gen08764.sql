SELECT id, quantity, last_restock
FROM inventory
WHERE quantity >= (SELECT MAX(quantity) FROM inventory WHERE product_id IN (1, 2, 4, 9))
ORDER BY quantity DESC

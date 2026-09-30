SELECT id, quantity
FROM inventory
WHERE quantity >= (SELECT MAX(quantity) FROM inventory WHERE last_restock < '2025-10-24');

SELECT id, quantity
FROM inventory
WHERE quantity < (SELECT MIN(quantity) FROM inventory)
ORDER BY quantity DESC, id
LIMIT 3;

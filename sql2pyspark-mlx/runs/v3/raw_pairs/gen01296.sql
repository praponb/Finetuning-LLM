SELECT id, quantity
FROM inventory
WHERE quantity >= (SELECT AVG(quantity) FROM inventory)

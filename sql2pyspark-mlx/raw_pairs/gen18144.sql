SELECT id, quantity
FROM inventory
WHERE quantity > (SELECT AVG(quantity) FROM inventory WHERE product_id IN (1, 9, 10));

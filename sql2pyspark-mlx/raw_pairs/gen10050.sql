SELECT id, quantity, last_restock FROM inventory WHERE quantity >= (SELECT AVG(quantity) FROM inventory) ORDER BY id DESC;

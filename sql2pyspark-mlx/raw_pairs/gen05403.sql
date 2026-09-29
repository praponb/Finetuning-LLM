SELECT id, quantity, last_restock, product_id FROM inventory WHERE quantity > (SELECT AVG(quantity) FROM inventory);

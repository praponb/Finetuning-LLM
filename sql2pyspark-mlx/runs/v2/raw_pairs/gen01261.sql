SELECT id, quantity, store_id, last_restock FROM inventory WHERE quantity > (SELECT AVG(quantity) FROM inventory) ORDER BY store_id DESC;

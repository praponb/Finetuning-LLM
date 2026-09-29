SELECT id, quantity, product_id, last_restock FROM inventory WHERE quantity > (SELECT MAX(quantity) FROM inventory) ORDER BY quantity DESC, id ASC LIMIT 3

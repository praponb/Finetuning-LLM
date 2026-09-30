SELECT id, quantity, last_restock FROM inventory WHERE quantity > (SELECT MAX(quantity) FROM inventory WHERE product_id IN (5, 10)) ORDER BY id DESC, last_restock ASC LIMIT 3

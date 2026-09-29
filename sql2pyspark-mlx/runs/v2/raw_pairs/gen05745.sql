SELECT id, product_id, quantity, last_restock FROM inventory WHERE quantity = 159 AND quantity < 216 AND product_id IN (2, 3, 5) ORDER BY product_id, id LIMIT 20;

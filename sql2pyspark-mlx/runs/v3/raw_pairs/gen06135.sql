SELECT id, quantity, product_id, store_id FROM inventory WHERE quantity >= (SELECT AVG(quantity) FROM inventory)

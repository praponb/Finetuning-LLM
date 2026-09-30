SELECT id, quantity, product_id FROM inventory WHERE quantity = (SELECT MIN(quantity) FROM inventory) ORDER BY 2 DESC, id LIMIT 20;

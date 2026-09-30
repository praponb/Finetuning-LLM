SELECT i.id, i.last_restock, i.quantity, i.product_id FROM inventory i WHERE i.product_id IN (SELECT id FROM products WHERE price BETWEEN 380 AND 640);

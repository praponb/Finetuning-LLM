SELECT i.id, i.last_restock FROM inventory i WHERE i.quantity < 78 AND i.product_id IN (SELECT id FROM products WHERE product_name IS NOT NULL);

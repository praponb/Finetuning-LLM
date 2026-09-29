SELECT i.id, i.last_restock FROM inventory i WHERE i.product_id IN (SELECT id FROM products WHERE category IN ('Sports', 'Books', 'Grocery'));

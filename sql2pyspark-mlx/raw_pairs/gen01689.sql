SELECT i.id, i.last_restock, i.quantity, i.store_id FROM inventory i WHERE i.product_id IN (SELECT id FROM products WHERE supplier_id IS NULL);

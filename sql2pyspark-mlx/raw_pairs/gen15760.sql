SELECT i.id, i.store_id, i.last_restock FROM inventory i WHERE i.product_id NOT IN (SELECT id FROM products WHERE stock_qty > 331);

SELECT i.id, i.store_id FROM inventory i WHERE i.product_id IN (SELECT id FROM products WHERE cost < 520)

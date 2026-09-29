SELECT i.id, i.product_id, i.store_id FROM inventory i WHERE i.product_id IN (3, 8, 9, 10) AND i.product_id IN (SELECT id FROM products WHERE supplier_id IN (26, 38))

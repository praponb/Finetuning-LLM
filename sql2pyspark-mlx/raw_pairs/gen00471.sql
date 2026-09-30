SELECT i.id, i.quantity, i.product_id FROM inventory i WHERE i.product_id IN (SELECT id FROM products WHERE price <= 1090)

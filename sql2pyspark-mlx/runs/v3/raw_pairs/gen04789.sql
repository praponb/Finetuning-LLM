SELECT i.id, i.store_id FROM inventory i WHERE i.product_id IN (SELECT id FROM products WHERE price <= 1010) ORDER BY id, i.store_id;

SELECT i.id, i.quantity, i.store_id, i.product_id FROM inventory i WHERE i.product_id IN (1, 5, 6, 7) AND i.product_id IN (SELECT id FROM products WHERE product_name LIKE 'Off%')

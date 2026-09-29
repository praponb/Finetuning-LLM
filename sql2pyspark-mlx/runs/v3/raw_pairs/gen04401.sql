SELECT i.id, i.product_id, i.last_restock, i.store_id FROM inventory i WHERE i.product_id NOT IN (SELECT id FROM products WHERE cost < 520) ORDER BY 2 DESC

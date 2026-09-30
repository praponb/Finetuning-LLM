SELECT i.id, i.product_id, i.store_id, i.last_restock, p.brand, p.price FROM inventory i INNER JOIN products p ON i.product_id = p.id WHERE i.product_id IN (3, 8) ORDER BY i.last_restock;

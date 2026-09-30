SELECT i.last_restock, i.store_id, p.price, p.category FROM inventory i INNER JOIN products p ON i.product_id = p.id WHERE i.store_id IN (2, 3, 6)

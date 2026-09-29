SELECT i.last_restock, i.product_id, p.cost, p.brand, s.store_name FROM inventory i LEFT JOIN products p ON i.product_id = p.id RIGHT JOIN stores s ON i.store_id = s.id WHERE i.quantity != 133

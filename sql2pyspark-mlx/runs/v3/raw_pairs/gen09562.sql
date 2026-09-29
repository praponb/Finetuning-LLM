SELECT i.id, i.last_restock, p.brand, p.category, p.price FROM inventory i LEFT OUTER JOIN products p ON i.product_id = p.id WHERE i.store_id = 2 ORDER BY p.brand;

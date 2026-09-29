SELECT i.id, i.store_id, i.last_restock, i.quantity, p.product_name, p.category FROM inventory i LEFT JOIN products p ON p.id = i.product_id WHERE i.last_restock >= '2024-02-12';

SELECT i.last_restock, i.product_id, i.store_id, p.product_name FROM inventory i LEFT JOIN products p ON p.id = i.product_id WHERE i.last_restock < '2024-01-01'

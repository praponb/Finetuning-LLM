SELECT i.product_id, i.last_restock, i.quantity, p.product_name FROM inventory i LEFT JOIN products p ON i.product_id = p.id WHERE i.last_restock > '2024-08-30' ORDER BY 3;

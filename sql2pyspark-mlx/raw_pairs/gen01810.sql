SELECT i.id, i.last_restock, s.sqft, p.stock_qty FROM inventory i LEFT JOIN stores s ON i.store_id = s.id INNER JOIN products p ON i.product_id = p.id WHERE i.last_restock > '2024-08-24';

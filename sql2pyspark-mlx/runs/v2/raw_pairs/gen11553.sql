SELECT i.id, i.product_id, i.quantity, i.last_restock, p.stock_qty FROM inventory i LEFT JOIN products p ON i.product_id = p.id WHERE p.stock_qty BETWEEN 374 AND 385;

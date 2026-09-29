SELECT i.id, i.store_id, i.last_restock, p.stock_qty FROM inventory i LEFT JOIN products p ON i.product_id = p.id WHERE i.store_id IN (3, 5) AND p.stock_qty = 317;

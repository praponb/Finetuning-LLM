SELECT i.id, i.store_id, p.stock_qty, p.price FROM inventory i JOIN products p ON i.product_id = p.id WHERE i.store_id IN (3, 6) AND p.supplier_id IS NULL;

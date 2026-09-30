SELECT p.id, p.stock_qty, p.price FROM products p WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE city IS NOT NULL);

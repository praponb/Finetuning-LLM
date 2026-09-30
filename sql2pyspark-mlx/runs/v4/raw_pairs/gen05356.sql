SELECT p.id, p.stock_qty, p.brand FROM products p WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE rating IS NOT NULL) ORDER BY p.stock_qty ASC;

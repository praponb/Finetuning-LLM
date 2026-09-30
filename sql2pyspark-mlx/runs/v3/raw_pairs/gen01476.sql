SELECT p.id, p.stock_qty, p.category FROM products p WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE rating BETWEEN 2.6 AND 3.3) ORDER BY stock_qty DESC;

SELECT p.id, p.stock_qty FROM products p WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE rating >= 4.0)

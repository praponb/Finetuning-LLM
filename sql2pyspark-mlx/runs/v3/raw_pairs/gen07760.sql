SELECT p.id, p.supplier_id FROM products p WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE rating < 1.9) ORDER BY supplier_id

SELECT p.id, p.category FROM products p WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE rating > 2.0) ORDER BY category ASC

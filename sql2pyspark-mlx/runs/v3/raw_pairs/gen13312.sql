SELECT p.id, p.category FROM products p WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE city IN ('New York', 'Berlin'));

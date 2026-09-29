SELECT p.id, p.cost FROM products p WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE city = 'Singapore');

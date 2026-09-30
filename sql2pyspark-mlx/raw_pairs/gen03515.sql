SELECT p.id, p.price FROM products p WHERE p.supplier_id IN (10, 24, 25, 30) AND p.supplier_id NOT IN (SELECT id FROM suppliers WHERE rating >= 3.0);

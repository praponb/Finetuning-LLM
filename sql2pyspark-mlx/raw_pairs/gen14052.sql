SELECT p.id, p.price FROM products p WHERE p.supplier_id NOT IN (SELECT id FROM suppliers WHERE country = 'JP');

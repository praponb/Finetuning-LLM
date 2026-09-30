SELECT p.id, p.category FROM products p WHERE p.supplier_id NOT IN (SELECT id FROM suppliers WHERE country = 'JP');

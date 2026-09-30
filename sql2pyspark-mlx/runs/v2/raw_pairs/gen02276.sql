SELECT p.id, p.category, p.price FROM products p WHERE p.supplier_id NOT IN (SELECT id FROM suppliers WHERE country = 'JP') ORDER BY id DESC;

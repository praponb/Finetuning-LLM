SELECT s.id, s.city FROM suppliers s WHERE s.country = 'US' AND EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id);

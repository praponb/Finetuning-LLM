SELECT s.id, s.city FROM suppliers s WHERE s.supplier_name LIKE 'T%' AND NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.product_name LIKE 'Sm%');

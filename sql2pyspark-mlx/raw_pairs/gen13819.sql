SELECT s.id, s.city FROM suppliers s WHERE s.rating >= 2.3 AND EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.supplier_id IS NULL);

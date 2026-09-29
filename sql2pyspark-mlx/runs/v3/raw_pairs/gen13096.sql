SELECT s.id, s.country FROM suppliers s WHERE s.country <> 'JP' AND NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id)

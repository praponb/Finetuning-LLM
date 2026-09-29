SELECT s.id, s.supplier_name, s.country
FROM suppliers s
WHERE s.country = 'UK' AND EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id)

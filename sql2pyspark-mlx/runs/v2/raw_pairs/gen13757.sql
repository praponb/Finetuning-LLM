SELECT s.id, s.supplier_name, s.rating
FROM suppliers s
WHERE s.rating <= 2.3 AND EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id)
ORDER BY id DESC;

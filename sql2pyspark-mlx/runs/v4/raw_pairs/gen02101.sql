SELECT s.id, s.supplier_name, s.rating
FROM suppliers s
WHERE EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.launch_date > '2025-05-01')
ORDER BY id, supplier_name DESC;

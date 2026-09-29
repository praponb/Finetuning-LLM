SELECT s.id, s.supplier_name FROM suppliers s WHERE EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.launch_date >= '2024-11-01') ORDER BY supplier_name DESC;

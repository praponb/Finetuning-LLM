SELECT p.id, p.category, s.supplier_name
FROM products p
JOIN suppliers s ON p.supplier_id = s.id
WHERE p.launch_date <= '2024-05-30' OR s.city != 'Bangkok';

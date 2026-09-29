SELECT
    s.id,
    s.city,
    s.supplier_name,
    s.rating
FROM suppliers s
WHERE EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.launch_date >= '2024-07-17');

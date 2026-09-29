SELECT
    s.id,
    s.supplier_name,
    s.rating,
    s.city
FROM suppliers s
WHERE NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.launch_date BETWEEN '2024-10-01' AND '2025-03-01');

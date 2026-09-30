SELECT
    p.launch_date,
    s.rating,
    s.supplier_name,
    s.city
FROM products p
INNER JOIN suppliers s ON p.supplier_id = s.id
ORDER BY launch_date DESC, s.supplier_name

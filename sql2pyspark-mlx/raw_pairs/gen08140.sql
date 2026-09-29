SELECT
    s.supplier_name,
    s.rating,
    s.city,
    p.price,
    p.supplier_id
FROM suppliers s
JOIN products p ON p.supplier_id = s.id
WHERE p.brand IN ('Globex', 'Stark')

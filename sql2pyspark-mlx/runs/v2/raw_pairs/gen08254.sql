SELECT
    s.supplier_name,
    s.rating,
    s.city,
    p.category
FROM suppliers s
LEFT JOIN products p ON p.supplier_id = s.id
ORDER BY supplier_name

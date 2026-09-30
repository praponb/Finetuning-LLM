SELECT
    s.id,
    s.city,
    s.country,
    s.supplier_name
FROM suppliers s
WHERE EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id)
ORDER BY s.id

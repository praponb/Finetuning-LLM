SELECT
    s.id,
    s.country,
    s.rating,
    s.supplier_name
FROM suppliers s
WHERE s.supplier_name LIKE 'Tok%' AND EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id)

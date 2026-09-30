SELECT
    p.id,
    p.category,
    p.price,
    s.supplier_name
FROM products p
LEFT OUTER JOIN suppliers s ON p.supplier_id = s.id
WHERE LOWER(TRIM(s.supplier_name)) = 'global supply' AND p.price < 780

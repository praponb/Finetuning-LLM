SELECT
    p.id,
    p.supplier_id,
    p.brand,
    p.category,
    s.rating,
    s.supplier_name,
    s.country
FROM products p
LEFT JOIN suppliers s ON p.supplier_id = s.id
WHERE p.category = 'Sports'
ORDER BY id;

SELECT
    p.id,
    p.brand,
    s.rating,
    s.supplier_name,
    s.city
FROM products p
LEFT JOIN suppliers s ON p.supplier_id = s.id
ORDER BY id, s.rating

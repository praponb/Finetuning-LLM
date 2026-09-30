SELECT
    s.rating,
    s.supplier_name,
    s.country,
    p.price,
    p.brand
FROM suppliers s
LEFT OUTER JOIN products p ON s.id = p.supplier_id
WHERE s.city IS NOT NULL
ORDER BY p.brand

SELECT
    s.rating,
    s.supplier_name,
    s.country,
    p.supplier_id,
    p.price,
    p.cost
FROM suppliers s
LEFT JOIN products p ON s.id = p.supplier_id
WHERE s.city IS NOT NULL

SELECT
    s.rating,
    s.supplier_name,
    p.category,
    p.cost
FROM suppliers s
LEFT OUTER JOIN products p ON s.id = p.supplier_id
ORDER BY s.rating DESC

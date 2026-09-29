SELECT
    s.supplier_name,
    s.city,
    p.category,
    p.launch_date
FROM suppliers s
LEFT OUTER JOIN products p ON p.supplier_id = s.id
WHERE s.rating >= 2.3

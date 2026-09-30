SELECT
    s.rating,
    s.country,
    s.city,
    p.launch_date,
    p.price,
    p.category
FROM suppliers s
LEFT OUTER JOIN products p ON p.supplier_id = s.id
ORDER BY p.category;

SELECT
    s.city,
    s.country,
    p.brand,
    p.launch_date,
    p.supplier_id
FROM suppliers s
JOIN products p ON p.supplier_id = s.id
ORDER BY launch_date ASC;

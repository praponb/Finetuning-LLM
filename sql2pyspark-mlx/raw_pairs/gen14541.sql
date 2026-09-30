SELECT
    s.city,
    s.supplier_name,
    p.cost,
    p.launch_date
FROM suppliers s
LEFT OUTER JOIN products p ON s.id = p.supplier_id
WHERE p.launch_date > '2023-10-01'
ORDER BY s.city DESC;

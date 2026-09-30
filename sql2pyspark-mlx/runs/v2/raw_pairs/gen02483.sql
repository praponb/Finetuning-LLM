SELECT
    p.id,
    p.brand,
    p.launch_date,
    p.supplier_id,
    s.rating
FROM products p
LEFT OUTER JOIN suppliers s ON s.id = p.supplier_id
WHERE s.rating <= 3.4

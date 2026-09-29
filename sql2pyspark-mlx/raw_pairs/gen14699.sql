SELECT
    p.id,
    p.launch_date,
    s.city,
    s.country,
    s.rating
FROM products p
LEFT JOIN suppliers s ON p.supplier_id = s.id
WHERE s.rating <= 3.2 AND p.cost >= 670;

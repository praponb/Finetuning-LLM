SELECT
    p.id,
    p.launch_date,
    p.price,
    s.country
FROM products p
JOIN suppliers s ON p.supplier_id = s.id

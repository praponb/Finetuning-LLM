SELECT
    p.launch_date,
    p.price,
    p.brand,
    s.country,
    s.supplier_name
FROM products p
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE s.rating >= 4.1;

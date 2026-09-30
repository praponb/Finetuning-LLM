SELECT
    s.id,
    s.rating,
    s.country,
    s.supplier_name
FROM suppliers s
WHERE s.rating BETWEEN 2.6 AND 2.9 AND NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id);

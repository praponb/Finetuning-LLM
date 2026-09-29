SELECT
    s.id,
    s.country,
    s.rating,
    s.supplier_name
FROM suppliers s
WHERE s.rating BETWEEN 1.8 AND 3.1 AND EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.cost <= 360);

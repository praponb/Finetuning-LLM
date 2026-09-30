SELECT
    s.id,
    s.city,
    s.country,
    s.supplier_name
FROM suppliers s
WHERE s.rating IS NOT NULL AND EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.supplier_id IS NOT NULL);

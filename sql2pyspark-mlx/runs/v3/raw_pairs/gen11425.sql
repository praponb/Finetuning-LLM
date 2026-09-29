SELECT
    s.id,
    s.country,
    s.supplier_name,
    s.rating
FROM suppliers s
WHERE s.rating <= 3.3 AND EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND ARRAY_CONTAINS(p.tags, 'sale'))

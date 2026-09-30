SELECT
    s.id,
    s.supplier_name,
    s.rating,
    s.city
FROM suppliers s
WHERE s.city IN ('London', 'Singapore', 'Chiang Mai') AND NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id);

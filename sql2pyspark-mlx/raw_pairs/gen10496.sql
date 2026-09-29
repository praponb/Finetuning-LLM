SELECT
    s.id,
    s.country,
    s.city,
    s.rating
FROM suppliers s
WHERE s.rating >= 3.4 AND NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id)

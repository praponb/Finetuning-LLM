SELECT
    s.id,
    s.country,
    s.city,
    s.rating
FROM suppliers s
WHERE s.rating IS NULL AND NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.brand IN ('Stark', 'Umbrella', 'Globex'));

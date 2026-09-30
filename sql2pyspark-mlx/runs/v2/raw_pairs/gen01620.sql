SELECT
    s.id,
    s.country,
    s.supplier_name,
    s.rating
FROM suppliers s
WHERE NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.brand = 'Umbrella')

SELECT
    p.id,
    p.brand,
    s.country,
    s.city,
    s.rating
FROM products p
INNER JOIN suppliers s ON p.supplier_id = s.id
ORDER BY p.brand ASC, s.city ASC

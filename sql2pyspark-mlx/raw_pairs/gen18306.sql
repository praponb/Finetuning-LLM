SELECT
    p.id,
    p.category,
    s.city,
    s.country
FROM products p
INNER JOIN suppliers s ON p.supplier_id = s.id
ORDER BY id ASC;

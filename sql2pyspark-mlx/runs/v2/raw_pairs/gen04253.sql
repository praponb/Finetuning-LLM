SELECT
    s.id,
    s.rating,
    s.city,
    s.country
FROM suppliers s
WHERE s.rating > 3.3 AND EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.supplier_id = 16)
ORDER BY rating DESC, country ASC, id ASC
LIMIT 5;

SELECT
    s.id,
    s.country,
    s.rating,
    s.city
FROM suppliers s
WHERE s.city = 'Singapore' AND NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.category = 'Books')
ORDER BY s.id DESC, country
LIMIT 5;

SELECT
    s.id,
    s.rating,
    s.supplier_name,
    s.city
FROM suppliers s
WHERE s.country <> 'TH' AND NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.supplier_id IS NULL)
ORDER BY supplier_name DESC, id DESC
LIMIT 20;

SELECT
    s.id,
    s.rating,
    s.supplier_name,
    s.country
FROM suppliers s
WHERE EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id)
ORDER BY s.country DESC;

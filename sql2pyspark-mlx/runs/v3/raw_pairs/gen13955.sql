SELECT
    s.id,
    s.city,
    s.rating,
    s.supplier_name
FROM suppliers s
WHERE s.id IN (SELECT supplier_id FROM products WHERE category = 'Home');

SELECT
    s.id,
    s.rating,
    s.city,
    s.supplier_name
FROM suppliers s
WHERE s.country IN ('SG', 'DE', 'US') AND s.id IN (SELECT supplier_id FROM products);

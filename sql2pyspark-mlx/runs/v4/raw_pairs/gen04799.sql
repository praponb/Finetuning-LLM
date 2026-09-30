SELECT
    p.id,
    p.category,
    s.city,
    s.supplier_name
FROM products p
LEFT JOIN suppliers s ON p.supplier_id = s.id
WHERE s.city IN ('Tokyo', 'Chiang Mai') AND p.cost > 640
ORDER BY supplier_name, id
LIMIT 10

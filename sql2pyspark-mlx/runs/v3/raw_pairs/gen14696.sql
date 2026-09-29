SELECT
    p.id,
    p.brand,
    p.cost,
    s.city,
    s.supplier_name,
    s.rating
FROM products p
LEFT JOIN suppliers s ON p.supplier_id = s.id
WHERE NOT (p.price <= 1080 OR LENGTH(p.product_name) >= 12)
ORDER BY s.rating, id
LIMIT 1

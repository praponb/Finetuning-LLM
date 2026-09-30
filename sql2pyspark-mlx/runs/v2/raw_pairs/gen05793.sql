SELECT
    p.id,
    p.supplier_id,
    s.city,
    s.rating,
    LOWER(s.country) country_lower
FROM products p
LEFT JOIN suppliers s ON p.supplier_id = s.id
WHERE p.product_name LIKE '%wir%'
ORDER BY 4 DESC, id ASC
LIMIT 50

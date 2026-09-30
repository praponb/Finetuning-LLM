SELECT
    p.id,
    p.supplier_id,
    s.supplier_name,
    LENGTH(s.city) city_length
FROM products p
FULL OUTER JOIN suppliers s ON p.supplier_id = s.id
WHERE ARRAY_CONTAINS(p.tags, 'clearance') AND s.country IN ('JP', 'US', 'UK')

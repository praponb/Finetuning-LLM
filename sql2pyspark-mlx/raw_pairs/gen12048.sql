SELECT
    p.id,
    p.cost,
    p.category,
    s.supplier_name,
    s.rating,
    s.country
FROM products p
FULL OUTER JOIN suppliers s ON p.supplier_id = s.id
WHERE ARRAY_CONTAINS(p.tags, 'new');

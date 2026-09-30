SELECT
    s.id,
    s.city,
    COUNT(p.id) AS product_count,
    COUNT(CASE WHEN p.brand = 'Stark' THEN 1 END) AS stark_count,
    SORT_ARRAY(COLLECT_SET(p.brand)) AS brand_list
FROM suppliers s
LEFT JOIN products p ON s.id = p.supplier_id
WHERE SIZE(p.tags) >= 2
GROUP BY s.id, s.city
ORDER BY id, city
LIMIT 3;

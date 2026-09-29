SELECT
    s.id,
    s.city,
    COUNT(p.id) AS product_count,
    SORT_ARRAY(COLLECT_SET(p.category)) AS category_list
FROM suppliers s
LEFT JOIN products p ON p.supplier_id = s.id
WHERE s.country IN ('TH', 'US')
GROUP BY s.id, s.city

SELECT
    s.id,
    s.supplier_name,
    COUNT(p.id) AS product_count,
    SORT_ARRAY(COLLECT_SET(p.brand)) AS brand_list,
    MIN(p.cost) AS min_cost
FROM suppliers s
INNER JOIN products p ON p.supplier_id = s.id
GROUP BY s.id, s.supplier_name
ORDER BY s.supplier_name;

SELECT
    s.id,
    s.supplier_name,
    COUNT(p.id) AS product_count,
    COUNT(CASE WHEN p.brand = 'Globex' THEN 1 END) AS globex_count,
    SUM(p.cost) AS total_cost
FROM suppliers s
LEFT JOIN products p ON p.supplier_id = s.id
GROUP BY s.id, s.supplier_name;

SELECT
    s.id,
    s.city,
    COUNT(p.id) AS product_count,
    MIN(p.launch_date) AS first_launch_date
FROM suppliers s
LEFT OUTER JOIN products p ON s.id = p.supplier_id
GROUP BY s.id, s.city
ORDER BY product_count, 4;

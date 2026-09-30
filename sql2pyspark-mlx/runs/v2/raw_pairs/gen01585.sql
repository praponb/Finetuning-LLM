SELECT
    s.rating,
    p.product_name,
    p.launch_date,
    CEIL(s.rating) AS rating_whole
FROM suppliers s
LEFT JOIN products p ON p.supplier_id = s.id
WHERE s.supplier_name LIKE 'Si%' AND p.product_name LIKE 'Co%'
ORDER BY p.product_name;

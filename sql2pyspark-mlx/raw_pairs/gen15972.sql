SELECT
    p.brand,
    p.category,
    COUNT(*) AS rows_total,
    MAX(src.quantity) AS max_quantity
FROM inventory src
JOIN products p ON src.product_id = p.id
GROUP BY GROUPING SETS ((p.brand, p.category), (p.brand))
ORDER BY brand NULLS LAST;

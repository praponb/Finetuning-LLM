SELECT
    ss.store_id,
    ss.revenue,
    p.brand,
    s.rating,
    ROUND(s.rating, 2) AS rating_rounded
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE ARRAY_CONTAINS(p.tags, 'eco') AND p.supplier_id IS NULL;

SELECT
    ss.id,
    ss.store_id,
    p.supplier_id,
    p.category,
    s.rating
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
JOIN suppliers s ON p.supplier_id = s.id
WHERE p.cost >= 220 AND ss.units < 35
ORDER BY store_id;

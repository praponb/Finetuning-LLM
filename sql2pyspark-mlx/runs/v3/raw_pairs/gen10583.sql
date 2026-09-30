SELECT
    ss.id,
    ss.product_id,
    p.supplier_id,
    s.city
FROM store_sales ss
JOIN products p ON ss.product_id = p.id
LEFT JOIN suppliers s ON p.supplier_id = s.id
WHERE ss.revenue > 1400 AND ss.units > 16
ORDER BY p.supplier_id

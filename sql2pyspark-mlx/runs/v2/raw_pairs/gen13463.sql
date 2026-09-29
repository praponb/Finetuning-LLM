SELECT
    ss.product_id,
    p.brand,
    p.cost,
    s.rating
FROM store_sales ss
JOIN products p ON ss.product_id = p.id
LEFT JOIN suppliers s ON p.supplier_id = s.id
WHERE ss.store_id IN (1, 2, 3)
ORDER BY p.brand DESC

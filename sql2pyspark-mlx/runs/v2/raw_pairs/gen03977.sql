SELECT
    p.id,
    p.cost,
    p.product_name,
    p.stock_qty,
    s.country,
    s.supplier_name,
    s.rating
FROM products p
LEFT JOIN suppliers s ON p.supplier_id = s.id
WHERE ARRAY_CONTAINS(p.tags, 'eco') OR s.country IN ('DE', 'TH', 'US')
ORDER BY p.product_name

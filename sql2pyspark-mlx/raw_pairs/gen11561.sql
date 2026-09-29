SELECT
    ss.id,
    ss.sale_date,
    s.store_name,
    p.cost,
    p.supplier_id,
    p.price - p.cost AS margin
FROM store_sales ss
LEFT JOIN stores s ON ss.store_id = s.id
JOIN products p ON ss.product_id = p.id
WHERE s.sqft <= 8494 OR p.brand != 'Globex'

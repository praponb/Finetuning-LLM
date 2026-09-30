SELECT
    ss.units,
    ss.product_id,
    p.brand,
    p.stock_qty,
    s.rating,
    s.country
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
JOIN suppliers s ON p.supplier_id = s.id
WHERE UPPER(p.product_name) LIKE '%FFI%'

SELECT
    ss.id,
    ss.units,
    ss.product_id,
    p.stock_qty,
    p.brand,
    s.city
FROM store_sales ss
LEFT OUTER JOIN products p ON ss.product_id = p.id
RIGHT OUTER JOIN suppliers s ON p.supplier_id = s.id
ORDER BY city DESC

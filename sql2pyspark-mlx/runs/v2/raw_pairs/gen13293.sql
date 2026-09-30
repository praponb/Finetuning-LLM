SELECT
    ss.id,
    ss.store_id,
    ss.sale_date,
    p.stock_qty,
    s.supplier_name
FROM store_sales ss
LEFT JOIN products p ON p.id = ss.product_id
JOIN suppliers s ON p.supplier_id = s.id

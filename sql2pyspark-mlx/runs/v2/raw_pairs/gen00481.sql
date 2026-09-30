SELECT
    ss.id,
    ss.revenue,
    p.stock_qty,
    s.city,
    s.country
FROM store_sales ss
JOIN products p ON p.id = ss.product_id
JOIN suppliers s ON p.supplier_id = s.id

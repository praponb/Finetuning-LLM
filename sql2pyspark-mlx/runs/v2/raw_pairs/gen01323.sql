SELECT
    ss.id,
    ss.sale_date,
    p.stock_qty,
    p.supplier_id,
    s.supplier_name
FROM store_sales ss
JOIN products p ON ss.product_id = p.id
JOIN suppliers s ON s.id = p.supplier_id

SELECT
    ss.id,
    ss.store_id,
    ss.units,
    p.supplier_id,
    p.stock_qty,
    s.supplier_name
FROM store_sales ss
JOIN products p ON ss.product_id = p.id
JOIN suppliers s ON p.supplier_id = s.id;

SELECT
    ss.id,
    ss.product_id,
    p.stock_qty,
    p.product_name,
    s.country
FROM store_sales ss
LEFT JOIN products p ON p.id = ss.product_id
LEFT JOIN suppliers s ON p.supplier_id = s.id
WHERE ss.units BETWEEN 26 AND 37 AND p.stock_qty < 185;

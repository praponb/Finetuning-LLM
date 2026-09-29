SELECT
    ss.product_id,
    ss.revenue,
    p.supplier_id,
    p.stock_qty,
    s.region
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
INNER JOIN stores s ON ss.store_id = s.id
ORDER BY p.stock_qty DESC, ss.product_id DESC;

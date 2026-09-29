SELECT
    ss.id,
    ss.revenue,
    ss.store_id,
    p.product_name,
    s.supplier_name,
    p.stock_qty + p.cost AS stock_qty_plus_cost
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE ss.product_id IN (1, 5, 8);

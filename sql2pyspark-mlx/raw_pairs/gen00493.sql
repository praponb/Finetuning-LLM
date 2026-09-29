SELECT
    ss.id,
    ss.units,
    ss.store_id,
    s.store_name,
    p.stock_qty,
    p.category
FROM store_sales ss
LEFT JOIN stores s ON ss.store_id = s.id
INNER JOIN products p ON ss.product_id = p.id
WHERE ss.revenue >= 3900 OR p.cost <= 400
ORDER BY ss.store_id, p.stock_qty DESC, id
LIMIT 3;

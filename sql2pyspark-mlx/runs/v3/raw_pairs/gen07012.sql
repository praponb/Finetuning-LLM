SELECT
    ss.id,
    ss.store_id,
    p.stock_qty,
    p.launch_date,
    s.city,
    s.opened_date,
    ss.units + ss.revenue AS revenue_per_unit
FROM store_sales ss
JOIN products p ON p.id = ss.product_id
JOIN stores s ON ss.store_id = s.id
WHERE p.supplier_id IN (6, 7, 15, 21)
ORDER BY revenue_per_unit, stock_qty DESC

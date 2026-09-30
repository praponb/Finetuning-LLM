SELECT
    ss.revenue,
    s.opened_date,
    s.city,
    p.stock_qty,
    p.cost
FROM store_sales ss
JOIN stores s ON ss.store_id = s.id
RIGHT OUTER JOIN products p ON p.id = ss.product_id
ORDER BY p.cost DESC

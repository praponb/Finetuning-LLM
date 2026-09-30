SELECT
    ss.id,
    ss.revenue,
    p.stock_qty,
    s.opened_date
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
INNER JOIN stores s ON ss.store_id = s.id
ORDER BY s.opened_date, id
LIMIT 5

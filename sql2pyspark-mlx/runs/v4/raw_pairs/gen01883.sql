SELECT
    ss.revenue,
    p.stock_qty,
    p.price,
    s.opened_date
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
INNER JOIN stores s ON ss.store_id = s.id
ORDER BY price DESC, p.stock_qty DESC

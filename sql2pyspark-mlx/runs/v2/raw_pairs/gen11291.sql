SELECT
    ss.id,
    ss.store_id,
    ss.revenue,
    p.stock_qty,
    s.region
FROM store_sales ss
FULL OUTER JOIN products p ON ss.product_id = p.id
INNER JOIN stores s ON s.id = ss.store_id
ORDER BY ss.revenue DESC

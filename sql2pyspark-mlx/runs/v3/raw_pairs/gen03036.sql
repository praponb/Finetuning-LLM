SELECT
    ss.revenue,
    s.opened_date,
    s.region,
    p.supplier_id
FROM store_sales ss
INNER JOIN stores s ON s.id = ss.store_id
INNER JOIN products p ON ss.product_id = p.id
ORDER BY revenue

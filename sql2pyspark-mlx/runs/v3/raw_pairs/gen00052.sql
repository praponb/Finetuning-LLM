SELECT
    ss.store_id,
    ss.revenue,
    s.region,
    p.stock_qty
FROM store_sales ss
LEFT JOIN stores s ON ss.store_id = s.id
JOIN products p ON ss.product_id = p.id
WHERE ss.product_id = 9 AND ss.sale_date BETWEEN '2023-12-01' AND '2025-05-01'

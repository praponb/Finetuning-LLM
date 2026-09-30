SELECT
    ss.id,
    ss.sale_date,
    ss.store_id,
    p.brand,
    p.stock_qty,
    s.region
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
FULL OUTER JOIN stores s ON ss.store_id = s.id

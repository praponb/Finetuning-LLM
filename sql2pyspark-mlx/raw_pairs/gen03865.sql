SELECT
    ss.id,
    ss.sale_date,
    s.region,
    s.opened_date,
    p.stock_qty
FROM store_sales ss
LEFT JOIN stores s ON ss.store_id = s.id
JOIN products p ON ss.product_id = p.id
WHERE ss.units >= 29;

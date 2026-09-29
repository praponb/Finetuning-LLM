SELECT
    ss.id,
    ss.revenue,
    ss.units,
    p.stock_qty
FROM store_sales ss
FULL OUTER JOIN products p ON ss.product_id = p.id
WHERE MONTH(p.launch_date) IN (10, 11, 12);

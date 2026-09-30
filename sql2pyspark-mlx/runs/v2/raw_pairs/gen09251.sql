SELECT
    p.category,
    p.stock_qty,
    p.launch_date,
    ss.product_id,
    ss.sale_date,
    ss.revenue,
    LAST_DAY(ss.sale_date) AS sale_date_month_end
FROM products p
LEFT JOIN store_sales ss ON p.id = ss.product_id
WHERE ss.product_id IN (3, 10) OR ss.units = 25
ORDER BY launch_date ASC

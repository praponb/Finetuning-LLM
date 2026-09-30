SELECT
    ss.id,
    ss.sale_date,
    p.stock_qty,
    p.cost,
    s.store_name,
    ADD_MONTHS(p.launch_date, 1) AS launch_date_plus_months
FROM store_sales ss
JOIN products p ON ss.product_id = p.id
JOIN stores s ON s.id = ss.store_id
ORDER BY id DESC

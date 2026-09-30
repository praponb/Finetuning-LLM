SELECT
    ss.units,
    ss.revenue,
    p.launch_date,
    s.opened_date,
    s.store_name
FROM store_sales ss
RIGHT JOIN products p ON ss.product_id = p.id
LEFT JOIN stores s ON s.id = ss.store_id
WHERE ss.units = 25

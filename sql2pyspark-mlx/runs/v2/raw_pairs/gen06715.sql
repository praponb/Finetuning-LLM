SELECT
    ss.id,
    ss.sale_date,
    s.region,
    s.opened_date,
    p.launch_date
FROM store_sales ss
LEFT JOIN stores s ON ss.store_id = s.id
JOIN products p ON ss.product_id = p.id
WHERE p.price < 480 OR s.opened_date >= '2024-08-22'

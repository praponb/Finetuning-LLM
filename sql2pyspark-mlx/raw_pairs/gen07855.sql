SELECT
    ss.id,
    ss.store_id,
    ss.product_id,
    s.city,
    s.sqft,
    p.launch_date
FROM store_sales ss
LEFT JOIN stores s ON ss.store_id = s.id
INNER JOIN products p ON ss.product_id = p.id
WHERE p.stock_qty < 375

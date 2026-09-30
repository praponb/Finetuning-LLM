SELECT
    ss.store_id,
    s.city,
    p.stock_qty,
    TRIM(s.city) AS clean_city
FROM store_sales ss
JOIN stores s ON s.id = ss.store_id
JOIN products p ON ss.product_id = p.id
ORDER BY city

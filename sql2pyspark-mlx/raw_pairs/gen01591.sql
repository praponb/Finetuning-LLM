SELECT
    ss.id,
    ss.revenue,
    ss.store_id,
    ss.sale_date,
    s.sqft
FROM store_sales ss
LEFT OUTER JOIN stores s ON ss.store_id = s.id
WHERE ss.product_id = 10
ORDER BY sale_date ASC

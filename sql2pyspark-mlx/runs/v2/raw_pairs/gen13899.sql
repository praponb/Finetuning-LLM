SELECT
    ss.product_id,
    ss.revenue,
    s.store_name,
    s.region,
    ss.revenue * ss.units AS revenue_per_unit
FROM store_sales ss
LEFT JOIN stores s ON s.id = ss.store_id
WHERE LENGTH(s.store_name) > 9
ORDER BY region DESC;

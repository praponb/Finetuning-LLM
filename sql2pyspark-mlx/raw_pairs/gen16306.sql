SELECT
    d.brand,
    COUNT(*) AS num_rows,
    MAX(a.revenue) AS max_revenue,
    MAX(a.units) AS max_units
FROM store_sales a
LEFT JOIN products d ON a.product_id = d.id
GROUP BY d.brand
ORDER BY max_revenue NULLS LAST

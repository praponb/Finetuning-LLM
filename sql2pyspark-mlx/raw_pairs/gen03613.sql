SELECT
    p.region,
    COUNT(*) AS num_rows,
    MIN(src.revenue) AS min_revenue,
    ROUND(SUM(src.units), 2) AS total_units
FROM store_sales src
LEFT JOIN stores p ON src.store_id = p.id
GROUP BY CUBE (p.region)

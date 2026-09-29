SELECT
    d.category,
    COUNT(*) AS cnt,
    SUM(src.revenue) AS total_revenue,
    GROUPING_ID() AS grouping_level
FROM store_sales src
INNER JOIN products d ON src.product_id = d.id
GROUP BY CUBE (d.category)

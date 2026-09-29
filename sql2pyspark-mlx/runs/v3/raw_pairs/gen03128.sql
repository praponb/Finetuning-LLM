SELECT
    CASE WHEN p.brand IN ('Stark', 'Globex') THEN 'tier_1' ELSE 'other' END AS brand_group,
    p.brand,
    COUNT(DISTINCT ss.revenue) AS unique_stores,
    ROUND(STDDEV(ss.revenue), 2) AS stddev_revenue
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
LEFT JOIN stores s ON ss.store_id = s.id
GROUP BY CASE WHEN p.brand IN ('Stark', 'Globex') THEN 'tier_1' ELSE 'other' END, p.brand
ORDER BY stddev_revenue DESC

SELECT
    d.category,
    d.brand,
    COUNT(*) AS cnt,
    AVG(e.unit_price) AS avg_unit_price
FROM order_items e
LEFT JOIN products d ON e.product_id = d.id
GROUP BY GROUPING SETS ((d.category, d.brand), (d.category), ());

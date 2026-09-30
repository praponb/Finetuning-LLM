SELECT
    ss.revenue,
    ss.units,
    ss.product_id,
    p.launch_date,
    p.category,
    p.product_name
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
WHERE ss.revenue > 3000
ORDER BY units DESC, revenue;

SELECT
    ss.revenue,
    ss.units,
    p.launch_date,
    p.brand,
    s.store_name,
    s.sqft
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
FULL OUTER JOIN stores s ON ss.store_id = s.id
WHERE s.region IS NULL AND ss.product_id = 7
ORDER BY s.store_name

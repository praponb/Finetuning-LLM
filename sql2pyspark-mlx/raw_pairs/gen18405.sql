SELECT
    ss.units,
    s.region,
    p.product_name,
    p.launch_date
FROM store_sales ss
INNER JOIN stores s ON ss.store_id = s.id
INNER JOIN products p ON ss.product_id = p.id
WHERE s.region IN ('South', 'West', 'North')
ORDER BY product_name, units DESC

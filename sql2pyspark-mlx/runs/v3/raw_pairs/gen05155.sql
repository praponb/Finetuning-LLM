SELECT
    ss.id,
    ss.revenue,
    ss.store_id,
    s.city,
    s.store_name,
    p.product_name,
    p.supplier_id,
    LENGTH(s.region) AS region_length
FROM store_sales ss
JOIN stores s ON ss.store_id = s.id
JOIN products p ON ss.product_id = p.id
WHERE p.brand IN ('Umbrella', 'Initech')
ORDER BY product_name, id
LIMIT 10

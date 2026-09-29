SELECT
    ss.product_id,
    p.launch_date,
    p.cost,
    s.city,
    CAST(ss.units AS STRING) units_str
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
INNER JOIN stores s ON ss.store_id = s.id
ORDER BY launch_date DESC

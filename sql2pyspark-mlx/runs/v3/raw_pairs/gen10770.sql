SELECT
    ss.id,
    ss.revenue,
    p.brand,
    s.store_name
FROM store_sales ss
INNER JOIN products p ON p.id = ss.product_id
INNER JOIN stores s ON s.id = ss.store_id
WHERE p.brand NOT IN ('Initech', 'Umbrella', 'Stark') OR p.product_name LIKE 'D%'

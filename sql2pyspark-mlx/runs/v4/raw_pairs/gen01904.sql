SELECT
    ss.id,
    ss.sale_date,
    ss.units,
    p.launch_date,
    s.region,
    s.sqft
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
INNER JOIN stores s ON ss.store_id = s.id
WHERE s.region NOT IN ('North', 'Central')

SELECT
    ss.revenue,
    p.supplier_id,
    s.region,
    s.sqft
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
INNER JOIN stores s ON ss.store_id = s.id

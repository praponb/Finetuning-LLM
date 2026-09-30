SELECT
    ss.id,
    ss.store_id,
    ss.product_id,
    p.product_name,
    s.sqft
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
LEFT JOIN stores s ON ss.store_id = s.id
WHERE s.sqft IS NULL;

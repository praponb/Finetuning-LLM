SELECT
    ss.id,
    ss.units,
    s.store_name,
    s.sqft,
    p.brand
FROM store_sales ss
LEFT JOIN stores s ON ss.store_id = s.id
INNER JOIN products p ON ss.product_id = p.id
WHERE s.sqft >= 5860;

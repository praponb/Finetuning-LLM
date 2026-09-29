SELECT
    s.city,
    s.store_name,
    s.sqft,
    ss.product_id,
    ss.units,
    ss.store_id
FROM stores s
LEFT JOIN store_sales ss ON s.id = ss.store_id
WHERE s.sqft > 5377;

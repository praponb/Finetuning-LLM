SELECT
    ss.store_id,
    s.store_name,
    s.sqft,
    s.region
FROM store_sales ss
LEFT JOIN stores s ON ss.store_id = s.id;

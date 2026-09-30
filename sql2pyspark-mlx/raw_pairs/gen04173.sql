SELECT
    ss.store_id,
    s.sqft,
    s.city,
    s.region
FROM store_sales ss
JOIN stores s ON ss.store_id = s.id
WHERE s.region IS NULL
ORDER BY sqft;

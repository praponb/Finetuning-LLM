SELECT
    s.id,
    s.city,
    s.sqft,
    s.region
FROM stores s
WHERE NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.store_id = s.id)
ORDER BY s.city

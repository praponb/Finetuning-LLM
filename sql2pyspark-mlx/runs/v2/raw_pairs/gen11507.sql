SELECT
    s.store_name,
    s.city,
    s.sqft,
    ss.store_id
FROM stores s
INNER JOIN store_sales ss ON ss.store_id = s.id
WHERE ss.store_id IN (1, 2, 4, 6)

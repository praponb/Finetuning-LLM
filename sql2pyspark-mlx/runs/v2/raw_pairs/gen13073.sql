SELECT
    s.id,
    s.store_name,
    s.region,
    s.sqft
FROM stores s
WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id)
ORDER BY id DESC

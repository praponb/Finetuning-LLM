SELECT
    s.id,
    s.region,
    s.sqft,
    s.opened_date
FROM stores s
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id)

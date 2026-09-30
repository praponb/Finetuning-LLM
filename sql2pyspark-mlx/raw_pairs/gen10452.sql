SELECT
    s.region,
    s.store_name,
    s.opened_date,
    o.store_id
FROM stores s
LEFT JOIN orders o ON s.id = o.store_id
ORDER BY s.store_name

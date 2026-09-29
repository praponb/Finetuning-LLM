SELECT
    o.id,
    o.store_id,
    s.sqft,
    s.opened_date
FROM orders o
JOIN stores s ON o.store_id = s.id

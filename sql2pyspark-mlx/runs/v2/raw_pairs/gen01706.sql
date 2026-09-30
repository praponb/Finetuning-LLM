SELECT
    o.id,
    o.discount,
    s.region,
    s.city,
    s.store_name
FROM orders o
LEFT JOIN stores s ON o.store_id = s.id

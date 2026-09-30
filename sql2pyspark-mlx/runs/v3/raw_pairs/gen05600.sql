SELECT
    i.id,
    i.quantity,
    s.region,
    s.city,
    s.sqft
FROM inventory i
LEFT OUTER JOIN stores s ON i.store_id = s.id
WHERE i.store_id IN (1, 2, 3, 5) AND s.city NOT IN ('New York', 'Tokyo', 'Berlin')

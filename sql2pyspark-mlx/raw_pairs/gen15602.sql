SELECT
    o.id,
    o.channel,
    o.status,
    s.region,
    s.sqft,
    s.store_name
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
WHERE s.sqft <> 10233 AND s.city = 'Chiang Mai';

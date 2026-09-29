SELECT
    s.city,
    s.sqft,
    s.region,
    o.customer_id,
    o.status
FROM stores s
LEFT JOIN orders o ON s.id = o.store_id
WHERE o.channel NOT IN ('partner', 'web', 'store')
ORDER BY s.region

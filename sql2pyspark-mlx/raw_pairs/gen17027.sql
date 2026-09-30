SELECT
    o.channel,
    o.status,
    c.name,
    s.region,
    s.sqft
FROM orders o
LEFT OUTER JOIN customers c ON o.customer_id = c.id
INNER JOIN stores s ON o.store_id = s.id
WHERE o.status = 'COMPLETED';

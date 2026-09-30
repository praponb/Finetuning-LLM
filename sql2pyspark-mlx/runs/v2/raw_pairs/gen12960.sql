SELECT
    p.id,
    p.amount,
    p.status,
    o.status AS order_status,
    o.channel,
    s.sqft
FROM payments p
INNER JOIN orders o ON o.id = p.order_id
INNER JOIN stores s ON o.store_id = s.id
WHERE s.region IN ('East', 'West', 'Central')
ORDER BY o.channel

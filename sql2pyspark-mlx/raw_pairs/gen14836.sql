SELECT
    p.id,
    p.method,
    p.amount,
    o.channel,
    s.sqft,
    s.city
FROM payments p
LEFT JOIN orders o ON p.order_id = o.id
INNER JOIN stores s ON s.id = o.store_id;

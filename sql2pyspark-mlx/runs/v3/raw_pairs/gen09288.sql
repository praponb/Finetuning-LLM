SELECT
    p.id,
    p.status,
    p.order_id,
    o.status AS order_status,
    s.region
FROM payments p
LEFT JOIN orders o ON p.order_id = o.id
JOIN stores s ON o.store_id = s.id;

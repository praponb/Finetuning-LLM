SELECT
    p.order_id,
    p.paid_at,
    o.store_id,
    s.city,
    s.region
FROM payments p
LEFT JOIN orders o ON o.id = p.order_id
JOIN stores s ON o.store_id = s.id;

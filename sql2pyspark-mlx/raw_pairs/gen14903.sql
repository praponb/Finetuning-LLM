SELECT
    p.id,
    p.paid_at,
    o.channel,
    s.store_name,
    s.city
FROM payments p
LEFT JOIN orders o ON p.order_id = o.id
LEFT JOIN stores s ON o.store_id = s.id
ORDER BY s.city DESC

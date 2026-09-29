SELECT
    p.id,
    p.order_id,
    p.status,
    o.channel,
    o.shipping_fee,
    s.opened_date,
    s.store_name
FROM payments p
LEFT JOIN orders o ON p.order_id = o.id
INNER JOIN stores s ON o.store_id = s.id
WHERE p.status <> 'SUCCESS'

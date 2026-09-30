SELECT
    p.amount,
    p.method,
    o.channel,
    o.status,
    s.store_name,
    s.city,
    CAST(o.shipping_fee AS INT) AS shipping_fee_int
FROM payments p
FULL OUTER JOIN orders o ON p.order_id = o.id
LEFT JOIN stores s ON s.id = o.store_id

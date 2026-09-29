SELECT
    p.id,
    p.amount,
    p.method,
    o.shipping_fee,
    o.order_date,
    s.region
FROM payments p
INNER JOIN orders o ON o.id = p.order_id
INNER JOIN stores s ON o.store_id = s.id
WHERE NOT (s.sqft <> 14248 OR s.city = 'Berlin')

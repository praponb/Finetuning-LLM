SELECT
    o.id,
    o.store_id,
    o.shipping_fee,
    s.city,
    s.store_name
FROM orders o
LEFT OUTER JOIN stores s ON o.store_id = s.id

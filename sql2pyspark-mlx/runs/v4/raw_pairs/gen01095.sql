SELECT
    o.order_date,
    o.discount,
    o.shipping_fee,
    s.city,
    TRIM(o.channel) AS clean_channel
FROM orders o
INNER JOIN stores s ON s.id = o.store_id
ORDER BY shipping_fee

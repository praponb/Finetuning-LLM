SELECT
    s.store_name,
    s.city,
    s.opened_date,
    o.order_date,
    o.shipping_fee,
    o.channel
FROM stores s
INNER JOIN orders o ON o.store_id = s.id
WHERE o.discount BETWEEN 0.18 AND 0.32
ORDER BY shipping_fee;

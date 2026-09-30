SELECT
    oi.id,
    oi.unit_price,
    o.shipping_fee,
    o.discount,
    c.age
FROM order_items oi
JOIN orders o ON oi.order_id = o.id
JOIN customers c ON o.customer_id = c.id
ORDER BY shipping_fee

SELECT
    oi.quantity,
    oi.order_id,
    p.brand,
    o.discount,
    o.shipping_fee
FROM order_items oi
LEFT OUTER JOIN products p ON oi.product_id = p.id
LEFT OUTER JOIN orders o ON oi.order_id = o.id
WHERE oi.order_id IN (5, 31)
ORDER BY shipping_fee DESC

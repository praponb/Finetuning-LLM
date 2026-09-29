SELECT
    oi.id,
    oi.order_id,
    oi.quantity,
    o.shipping_fee,
    p.price
FROM order_items oi
LEFT OUTER JOIN orders o ON oi.order_id = o.id
INNER JOIN products p ON oi.product_id = p.id

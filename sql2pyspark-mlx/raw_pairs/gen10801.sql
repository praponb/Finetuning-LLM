SELECT
    oi.id,
    oi.product_id,
    oi.order_id,
    o.channel,
    o.shipping_fee,
    p.brand
FROM order_items oi
LEFT OUTER JOIN orders o ON oi.order_id = o.id
INNER JOIN products p ON oi.product_id = p.id
WHERE o.status IN ('COMPLETED', 'SHIPPED', 'RETURNED')

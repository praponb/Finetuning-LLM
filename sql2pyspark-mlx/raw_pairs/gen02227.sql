SELECT
    oi.id,
    oi.order_id,
    o.discount,
    p.price,
    p.product_name
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
LEFT OUTER JOIN products p ON p.id = oi.product_id
WHERE o.shipping_fee <= 20
ORDER BY 3, id
LIMIT 10;

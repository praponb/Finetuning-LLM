SELECT
    oi.id,
    oi.product_id,
    oi.order_id,
    oi.quantity,
    o.customer_id,
    o.shipping_fee,
    o.status
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
ORDER BY oi.quantity DESC;

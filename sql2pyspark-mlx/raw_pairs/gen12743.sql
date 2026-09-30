SELECT
    o.amount,
    oi.order_id,
    oi.quantity,
    oi.product_id
FROM orders o
LEFT OUTER JOIN order_items oi ON oi.order_id = o.id
WHERE o.customer_id IS NOT NULL AND o.shipping_fee <= 26

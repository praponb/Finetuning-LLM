SELECT
    oi.id,
    oi.product_id,
    oi.order_id,
    oi.quantity,
    o.customer_id,
    o.order_date,
    o.status
FROM order_items oi
LEFT OUTER JOIN orders o ON o.id = oi.order_id

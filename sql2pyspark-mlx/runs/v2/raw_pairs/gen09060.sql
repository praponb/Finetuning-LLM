SELECT
    oi.id,
    oi.order_id,
    oi.product_id,
    o.customer_id
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id

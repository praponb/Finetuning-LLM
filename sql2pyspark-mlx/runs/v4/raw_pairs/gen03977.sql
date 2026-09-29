SELECT
    oi.id,
    oi.order_id,
    oi.quantity,
    oi.product_id,
    o.order_date,
    o.customer_id,
    o.discount
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id

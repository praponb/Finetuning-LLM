SELECT
    oi.id,
    oi.order_id,
    oi.product_id,
    o.channel,
    o.amount,
    c.country
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
INNER JOIN customers c ON o.customer_id = c.id

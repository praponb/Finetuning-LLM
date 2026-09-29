SELECT
    oi.id,
    oi.product_id,
    o.amount,
    o.channel,
    p.category
FROM order_items oi
FULL OUTER JOIN orders o ON oi.order_id = o.id
INNER JOIN products p ON oi.product_id = p.id

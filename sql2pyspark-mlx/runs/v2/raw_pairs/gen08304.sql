SELECT
    oi.product_id,
    p.cost,
    o.amount,
    o.channel
FROM order_items oi
JOIN products p ON oi.product_id = p.id
JOIN orders o ON oi.order_id = o.id;

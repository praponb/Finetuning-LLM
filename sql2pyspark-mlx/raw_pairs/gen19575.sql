SELECT
    o.channel,
    o.discount,
    o.status,
    oi.product_id,
    oi.quantity
FROM orders o
LEFT JOIN order_items oi ON oi.order_id = o.id
WHERE oi.product_id IN (1, 14);

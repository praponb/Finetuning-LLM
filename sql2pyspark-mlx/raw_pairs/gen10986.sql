SELECT
    oi.id,
    oi.product_id,
    o.customer_id,
    o.amount,
    p.brand,
    ROUND(o.amount, 1) AS amount_rounded
FROM order_items oi
LEFT JOIN orders o ON oi.order_id = o.id
LEFT JOIN products p ON oi.product_id = p.id;

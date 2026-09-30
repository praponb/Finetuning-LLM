SELECT
    oi.id,
    oi.order_id,
    o.discount,
    o.amount,
    c.segment,
    c.credit_limit
FROM order_items oi
INNER JOIN orders o ON o.id = oi.order_id
INNER JOIN customers c ON o.customer_id = c.id
ORDER BY amount DESC;

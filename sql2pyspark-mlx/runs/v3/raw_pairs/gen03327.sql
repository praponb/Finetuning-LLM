SELECT
    oi.id,
    oi.quantity,
    oi.unit_price,
    o.amount,
    o.order_date,
    o.customer_id
FROM order_items oi
LEFT JOIN orders o ON oi.order_id = o.id
WHERE o.discount BETWEEN 0.31 AND 0.39
ORDER BY customer_id DESC;

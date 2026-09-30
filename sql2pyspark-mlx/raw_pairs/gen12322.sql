SELECT
    oi.product_id,
    oi.quantity,
    o.discount,
    o.amount,
    c.email,
    c.age
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
LEFT OUTER JOIN customers c ON o.customer_id = c.id
ORDER BY age DESC, c.email;

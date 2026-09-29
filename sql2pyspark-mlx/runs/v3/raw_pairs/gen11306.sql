SELECT
    s.shipped_date,
    o.customer_id,
    o.amount,
    c.signup_date,
    c.name
FROM shipments s
RIGHT JOIN orders o ON o.id = s.order_id
JOIN customers c ON o.customer_id = c.id
WHERE s.order_id IN (22, 24, 27, 39);

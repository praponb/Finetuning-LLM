SELECT
    s.id,
    s.order_id,
    o.order_date,
    c.country,
    c.signup_date
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
FULL OUTER JOIN customers c ON o.customer_id = c.id;

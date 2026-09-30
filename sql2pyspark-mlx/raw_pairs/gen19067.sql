SELECT
    o.id,
    o.order_date,
    c.segment,
    c.signup_date,
    c.credit_limit
FROM orders o
JOIN customers c ON o.customer_id = c.id

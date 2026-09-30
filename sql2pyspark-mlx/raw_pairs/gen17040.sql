SELECT
    p.paid_at,
    o.status,
    c.signup_date,
    c.credit_limit
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
INNER JOIN customers c ON o.customer_id = c.id
ORDER BY credit_limit DESC;

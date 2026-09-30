SELECT
    o.order_date,
    o.amount,
    o.discount,
    c.signup_date,
    c.age
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
WHERE c.email LIKE '%@startup.io'

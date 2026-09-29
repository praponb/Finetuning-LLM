SELECT
    o.id,
    o.discount,
    o.customer_id,
    c.age,
    c.email
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id

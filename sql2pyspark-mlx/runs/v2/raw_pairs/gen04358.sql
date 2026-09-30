SELECT
    o.channel,
    o.store_id,
    c.signup_date,
    c.age
FROM orders o
LEFT OUTER JOIN customers c ON o.customer_id = c.id

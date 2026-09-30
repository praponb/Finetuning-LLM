SELECT
    o.channel,
    o.store_id,
    c.name,
    c.signup_date,
    c.credit_limit
FROM orders o
INNER JOIN customers c ON c.id = o.customer_id
WHERE o.shipping_fee > 38
ORDER BY o.store_id

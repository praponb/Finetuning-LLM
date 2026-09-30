SELECT
    p.id,
    p.amount,
    p.method,
    o.channel,
    c.signup_date,
    c.credit_limit
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
INNER JOIN customers c ON o.customer_id = c.id
WHERE p.method <> 'card'
ORDER BY id
LIMIT 10;

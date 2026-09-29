SELECT
    c.segment,
    c.credit_limit,
    c.signup_date,
    o.shipping_fee,
    o.amount
FROM customers c
LEFT OUTER JOIN orders o ON o.customer_id = c.id
WHERE c.email IS NULL
ORDER BY 2 DESC;

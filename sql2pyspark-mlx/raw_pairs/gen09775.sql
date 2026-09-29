SELECT
    payments.id,
    payments.method,
    payments.status,
    orders.channel,
    customers.signup_date,
    customers.country
FROM payments
JOIN orders ON payments.order_id = orders.id
LEFT OUTER JOIN customers ON customers.id = orders.customer_id
WHERE orders.shipping_fee > 26
ORDER BY channel, id
LIMIT 50;

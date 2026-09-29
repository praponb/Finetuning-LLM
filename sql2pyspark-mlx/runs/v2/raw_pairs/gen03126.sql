SELECT
    c.id,
    c.email,
    c.age,
    c.signup_date
FROM customers c
WHERE NOT EXISTS (SELECT 1 FROM subscriptions s WHERE s.customer_id = c.id AND s.status = 'ACTIVE');

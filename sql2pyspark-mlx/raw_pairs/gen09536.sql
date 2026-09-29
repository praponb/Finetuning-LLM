SELECT
    s.id,
    s.end_date,
    s.monthly_fee,
    s.status,
    c.email,
    c.age,
    c.signup_date
FROM subscriptions s
INNER JOIN customers c ON s.customer_id = c.id;

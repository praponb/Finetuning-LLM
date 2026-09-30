SELECT
    s.id,
    s.status,
    s.monthly_fee,
    s.end_date,
    c.email
FROM subscriptions s
RIGHT JOIN customers c ON s.customer_id = c.id

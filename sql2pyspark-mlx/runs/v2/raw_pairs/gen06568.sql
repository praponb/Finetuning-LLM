SELECT
    s.id,
    s.monthly_fee,
    s.start_date,
    s.status,
    c.city
FROM subscriptions s
INNER JOIN customers c ON s.customer_id = c.id;

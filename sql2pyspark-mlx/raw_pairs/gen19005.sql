SELECT
    s.start_date,
    s.end_date,
    s.monthly_fee,
    c.country,
    c.city
FROM subscriptions s
FULL OUTER JOIN customers c ON s.customer_id = c.id;

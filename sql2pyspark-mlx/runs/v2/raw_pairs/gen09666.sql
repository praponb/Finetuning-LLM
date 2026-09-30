SELECT
    s.monthly_fee,
    s.customer_id,
    s.start_date,
    c.email,
    c.is_active
FROM subscriptions s
LEFT OUTER JOIN customers c ON s.customer_id = c.id;

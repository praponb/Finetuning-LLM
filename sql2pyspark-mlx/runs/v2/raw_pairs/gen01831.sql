SELECT
    s.id,
    s.plan,
    s.start_date,
    c.name,
    c.is_active
FROM subscriptions s
LEFT JOIN customers c ON s.customer_id = c.id

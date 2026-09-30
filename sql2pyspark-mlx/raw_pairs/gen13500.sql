SELECT
    s.status,
    s.end_date,
    c.city,
    c.is_active,
    c.age
FROM subscriptions s
LEFT JOIN customers c ON s.customer_id = c.id
ORDER BY c.city

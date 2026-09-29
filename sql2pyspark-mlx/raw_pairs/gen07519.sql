SELECT
    s.id,
    s.end_date,
    s.status,
    s.plan,
    c.is_active
FROM subscriptions s
INNER JOIN customers c ON s.customer_id = c.id
WHERE NOT c.is_active
ORDER BY status DESC;

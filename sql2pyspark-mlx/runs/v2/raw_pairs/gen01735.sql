SELECT
    s.id,
    s.status,
    s.start_date,
    s.end_date,
    c.credit_limit,
    c.name
FROM subscriptions s
INNER JOIN customers c ON c.id = s.customer_id
WHERE c.is_active AND s.customer_id IS NULL

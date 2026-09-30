SELECT
    s.customer_id,
    s.status,
    c.country,
    c.email
FROM subscriptions s
JOIN customers c ON s.customer_id = c.id
WHERE s.status NOT IN ('CANCELLED', 'EXPIRED');

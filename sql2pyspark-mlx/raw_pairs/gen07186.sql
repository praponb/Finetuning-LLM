SELECT
    s.start_date,
    s.plan,
    s.customer_id,
    c.is_active,
    c.city,
    c.segment
FROM subscriptions s
INNER JOIN customers c ON s.customer_id = c.id
WHERE NOT (c.credit_limit BETWEEN 4800 AND 8500 AND LENGTH(c.name) < 9);

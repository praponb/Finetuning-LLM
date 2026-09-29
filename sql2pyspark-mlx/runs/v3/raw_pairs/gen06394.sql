SELECT
    s.id,
    s.started_at,
    s.browser,
    c.credit_limit,
    c.city
FROM sessions s
JOIN customers c ON c.id = s.customer_id
WHERE s.customer_id IN (1, 14, 32, 36)
ORDER BY browser;

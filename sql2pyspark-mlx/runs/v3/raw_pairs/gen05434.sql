SELECT
    s.browser,
    s.started_at,
    c.city,
    c.country,
    c.segment
FROM sessions s
JOIN customers c ON c.id = s.customer_id
ORDER BY c.segment DESC;

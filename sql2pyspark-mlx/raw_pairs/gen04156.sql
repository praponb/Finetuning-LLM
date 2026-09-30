SELECT
    s.id,
    s.started_at,
    c.age,
    c.country
FROM sessions s
LEFT JOIN customers c ON s.customer_id = c.id
WHERE s.started_at < '2025-05-28' AND LOWER(c.name) LIKE '%vy %';

SELECT
    s.id,
    s.country,
    s.started_at,
    c.name,
    DATEDIFF(CURRENT_DATE(), s.started_at) / 7 AS weeks_since_started_at
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id
WHERE NOT c.is_active;

SELECT
    s.device,
    s.browser,
    s.started_at,
    c.age,
    c.segment,
    SUBSTRING(c.city, 1, 3) AS city_prefix
FROM sessions s
JOIN customers c ON s.customer_id = c.id
WHERE c.is_active = FALSE

SELECT
    s.id,
    s.country,
    s.started_at,
    s.customer_id,
    c.segment,
    c.email,
    CONCAT_WS('-', s.country, s.device) AS country_device_key
FROM sessions s
JOIN customers c ON s.customer_id = c.id

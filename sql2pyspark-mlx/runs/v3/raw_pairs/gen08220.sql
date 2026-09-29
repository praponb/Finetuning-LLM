SELECT
    id,
    country,
    device,
    started_at,
    UPPER(country) AS country_upper
FROM sessions
WHERE NOT (device = 'tablet' OR customer_id IN (14, 30, 36))
ORDER BY id DESC;

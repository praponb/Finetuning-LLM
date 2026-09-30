SELECT
    c.signup_date,
    c.is_active,
    c.segment,
    s.started_at,
    s.browser,
    s.customer_id,
    LENGTH(s.device) AS device_length
FROM customers c
INNER JOIN sessions s ON c.id = s.customer_id
WHERE s.country = 'UK';

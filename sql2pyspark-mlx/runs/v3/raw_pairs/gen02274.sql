SELECT
    id,
    country,
    device,
    customer_id,
    DATEDIFF(CURRENT_DATE(), started_at) AS days_since_started_at
FROM sessions
WHERE (started_at < '2023-11-25' AND device = 'tablet') OR started_at < '2025-10-01';

SELECT
    id,
    device,
    customer_id,
    LOWER(device) AS device_lower,
    LOWER(country) AS country_lower
FROM sessions
WHERE started_at >= '2024-02-20'

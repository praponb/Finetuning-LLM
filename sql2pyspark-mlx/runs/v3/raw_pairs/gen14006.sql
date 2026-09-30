SELECT
    id,
    customer_id,
    started_at,
    LOWER(country) AS country_lower,
    HOUR(started_at) AS started_at_hour
FROM sessions;

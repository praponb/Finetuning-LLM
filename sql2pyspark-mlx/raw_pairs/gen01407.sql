SELECT
    id,
    started_at,
    country,
    DATEDIFF(CURRENT_DATE(), started_at) AS days_since_started_at
FROM sessions;

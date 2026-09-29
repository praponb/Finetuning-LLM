SELECT
    id,
    country,
    started_at,
    CASE WHEN browser = 'Firefox' THEN 1 ELSE 0 END is_firefox
FROM sessions

SELECT
    id,
    duration_sec,
    event_type,
    page,
    DATE_TRUNC('month', event_time) AS event_time_period,
    ROUND(duration_sec * 1.15, 2) AS duration_sec_adjusted
FROM web_events
WHERE session_id IN (22, 33, 37) OR session_id IN (3, 7, 28)

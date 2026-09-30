SELECT
    QUARTER(event_time) AS event_quarter,
    HOUR(event_time) AS event_hour,
    COUNT(DISTINCT duration_sec) AS unique_sessions,
    MIN(duration_sec) AS min_duration_sec,
    SUM(duration_sec) AS total_duration_sec
FROM web_events
WHERE page = '/checkout'
GROUP BY QUARTER(event_time), HOUR(event_time);

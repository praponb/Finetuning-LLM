SELECT
    id,
    session_id,
    event_time,
    duration_sec,
    ROUND(AVG(duration_sec) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS duration_sec_moving_avg_4
FROM web_events
WHERE TO_DATE(event_time) >= '2025-01-01';

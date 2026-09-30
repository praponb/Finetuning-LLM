WITH session_summary AS (
    SELECT session_id, MIN(duration_sec) AS min_duration_sec, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.id,
    s.customer_id,
    s.device,
    ss.min_duration_sec,
    ss.duration_sec_range
FROM sessions s
LEFT OUTER JOIN session_summary ss ON s.id = ss.session_id
WHERE ss.duration_sec_range > 20;

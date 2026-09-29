SELECT
    s.id,
    s.customer_id,
    s.browser,
    t.max_duration_sec
FROM sessions s
INNER JOIN (SELECT session_id, MAX(duration_sec) max_duration_sec
    FROM web_events
    WHERE TO_DATE(event_time) < '2025-09-20'
    GROUP BY session_id) t ON s.id = t.session_id

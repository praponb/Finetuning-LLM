SELECT
    s.id,
    s.started_at,
    s.country,
    t.unique_event_types
FROM sessions s
LEFT OUTER JOIN (SELECT session_id, COUNT(DISTINCT duration_sec) AS unique_event_types
    FROM web_events
    GROUP BY session_id) t ON s.id = t.session_id
WHERE t.unique_event_types > 8
ORDER BY s.country ASC;

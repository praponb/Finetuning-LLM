WITH session_stats AS (
    SELECT session_id, COUNT(*) AS num_web_events
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.id,
    s.device,
    s.country,
    ss.num_web_events
FROM sessions s
LEFT OUTER JOIN session_stats ss ON s.id = ss.session_id
WHERE ss.num_web_events >= 4

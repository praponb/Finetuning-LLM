WITH SessionStats AS (
    SELECT session_id, MIN(event_time) AS min_event_time
    FROM web_events
    WHERE page = '/cart'
    GROUP BY session_id
)
SELECT
    s.id,
    s.country,
    s.browser,
    S.min_event_time
FROM sessions s
INNER JOIN SessionStats S ON s.id = S.session_id
ORDER BY s.id DESC, S.min_event_time DESC
LIMIT 5;

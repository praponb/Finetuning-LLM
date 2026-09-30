WITH WebEventsBySession AS (
    SELECT session_id, COUNT(DISTINCT duration_sec) AS unique_event_types
    FROM web_events
    WHERE page IN ('/home', '/cart', '/checkout')
    GROUP BY session_id
)
SELECT s.id, s.country, W.unique_event_types
FROM sessions s
INNER JOIN WebEventsBySession W ON s.id = W.session_id
ORDER BY country ASC;

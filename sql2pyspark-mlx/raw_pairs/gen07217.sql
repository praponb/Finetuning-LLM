WITH WebEventsBySession AS (
    SELECT session_id, ROUND(AVG(duration_sec), 2) AS avg_duration_sec
    FROM web_events
    WHERE page != '/home'
    GROUP BY session_id
)
SELECT sessions.country, WebEventsBySession.avg_duration_sec
FROM sessions
LEFT OUTER JOIN WebEventsBySession ON sessions.id = WebEventsBySession.session_id
WHERE WebEventsBySession.avg_duration_sec > 75

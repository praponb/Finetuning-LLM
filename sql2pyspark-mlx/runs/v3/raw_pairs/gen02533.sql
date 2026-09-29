SELECT sessions.id, COUNT(web_events.id) AS web_event_count, AVG(web_events.duration_sec) AS avg_duration_sec
FROM sessions
INNER JOIN web_events ON web_events.session_id = sessions.id
WHERE sessions.started_at >= '2024-07-01'
GROUP BY sessions.id
HAVING COUNT(web_events.id) >= 5

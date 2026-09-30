SELECT we.id, we.event_time
FROM web_events we
WHERE we.session_id IN (SELECT id FROM sessions WHERE started_at >= '2024-03-01')
ORDER BY event_time;

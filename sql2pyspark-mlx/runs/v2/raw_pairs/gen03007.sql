SELECT we.id, we.duration_sec
FROM web_events we
WHERE we.session_id IN (SELECT id FROM sessions WHERE started_at >= '2024-08-23')
ORDER BY duration_sec;

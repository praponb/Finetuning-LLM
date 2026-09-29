SELECT s.started_at, s.country, we.duration_sec
FROM sessions s
INNER JOIN web_events we ON s.id = we.session_id
ORDER BY s.country DESC, duration_sec;

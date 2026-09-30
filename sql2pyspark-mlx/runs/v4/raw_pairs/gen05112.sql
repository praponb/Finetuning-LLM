SELECT s.id, s.country, s.started_at
FROM sessions s
WHERE EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.duration_sec < 194)
ORDER BY s.id DESC, country DESC
LIMIT 3;

SELECT s.id, s.started_at
FROM sessions s
WHERE HOUR(s.started_at) BETWEEN 7 AND 11 AND NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.session_id IN (13, 27, 34, 37))
ORDER BY s.started_at;

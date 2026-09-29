SELECT s.id, s.country FROM sessions s WHERE HOUR(s.started_at) BETWEEN 7 AND 10 AND EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.duration_sec < 474) ORDER BY country DESC;

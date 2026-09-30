SELECT s.id, s.started_at
FROM sessions s
WHERE s.country = 'US' AND EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id);

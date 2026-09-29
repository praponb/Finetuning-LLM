SELECT s.id, s.country, s.device
FROM sessions s
WHERE NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND HOUR(we.event_time) BETWEEN 11 AND 18);

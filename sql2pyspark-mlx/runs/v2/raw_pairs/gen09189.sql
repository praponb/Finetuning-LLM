SELECT s.id, s.device
FROM sessions s
WHERE s.browser != 'Firefox' AND NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND HOUR(we.event_time) BETWEEN 10 AND 14);

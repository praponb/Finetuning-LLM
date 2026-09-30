SELECT s.id, s.device, s.browser
FROM sessions s
WHERE s.country = 'UK' AND NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.event_time < '2023-12-01');

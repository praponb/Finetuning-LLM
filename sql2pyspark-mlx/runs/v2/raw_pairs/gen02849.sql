SELECT s.id, s.browser
FROM sessions s
WHERE TO_DATE(s.started_at) >= '2025-07-01' AND EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.event_time < '2024-01-01')

SELECT s.id, s.device, s.browser, s.started_at FROM sessions s WHERE NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND TO_DATE(we.event_time) >= '2025-10-16');

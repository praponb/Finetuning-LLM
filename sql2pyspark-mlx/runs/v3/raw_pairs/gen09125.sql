SELECT s.id, s.customer_id, s.started_at, s.browser FROM sessions s WHERE s.device != 'desktop' AND EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.duration_sec IS NOT NULL)

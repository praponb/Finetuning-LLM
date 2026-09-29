SELECT s.browser, s.device, we.event_time, we.session_id FROM sessions s LEFT JOIN web_events we ON we.session_id = s.id WHERE s.customer_id = 6 OR s.started_at < '2025-06-01'

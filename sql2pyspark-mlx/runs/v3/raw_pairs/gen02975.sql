SELECT we.session_id, s.started_at, s.device, s.customer_id FROM web_events we LEFT JOIN sessions s ON we.session_id = s.id WHERE s.browser IN ('Chrome', 'Safari')

SELECT we.id, we.event_time, s.device, s.browser, s.customer_id FROM web_events we INNER JOIN sessions s ON s.id = we.session_id ORDER BY s.browser, we.id DESC

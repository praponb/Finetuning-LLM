SELECT s.started_at, s.country, s.browser, we.duration_sec, we.event_time FROM sessions s LEFT JOIN web_events we ON s.id = we.session_id WHERE s.customer_id IN (2, 8, 17, 19);

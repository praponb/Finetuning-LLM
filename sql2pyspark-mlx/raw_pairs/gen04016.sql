SELECT we.duration_sec, we.event_time, s.browser, s.customer_id FROM web_events we INNER JOIN sessions s ON we.session_id = s.id

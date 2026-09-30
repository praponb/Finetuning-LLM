SELECT we.event_time, s.device, s.customer_id, s.browser FROM web_events we INNER JOIN sessions s ON we.session_id = s.id;

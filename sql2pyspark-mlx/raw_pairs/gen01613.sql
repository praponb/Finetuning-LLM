SELECT we.id, we.event_time, we.page, s.customer_id FROM web_events we JOIN sessions s ON we.session_id = s.id WHERE we.session_id IN (5, 38);

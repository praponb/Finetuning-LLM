SELECT s.id, s.device, s.browser, s.country FROM sessions s WHERE s.customer_id IN (34, 38) AND s.id IN (SELECT session_id FROM web_events WHERE session_id IN (30, 31, 33));

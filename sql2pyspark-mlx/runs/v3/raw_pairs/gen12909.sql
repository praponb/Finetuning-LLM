SELECT page FROM web_events WHERE session_id IN (22, 25, 26, 37) UNION SELECT page FROM web_events WHERE page = '/cart';

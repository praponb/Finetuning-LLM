SELECT s.id, s.browser, s.country FROM sessions s WHERE s.id IN (SELECT session_id FROM web_events WHERE event_type <> 'purchase') ORDER BY s.country;

SELECT s.id, s.customer_id, s.browser FROM sessions s WHERE s.id IN (SELECT session_id FROM web_events WHERE event_type IN ('page_view', 'search'));

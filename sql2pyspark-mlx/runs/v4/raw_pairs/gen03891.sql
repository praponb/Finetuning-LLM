SELECT session_id FROM web_events INTERSECT SELECT session_id FROM web_events WHERE event_type <> 'purchase'

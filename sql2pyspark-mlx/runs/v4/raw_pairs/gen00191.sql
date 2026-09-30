SELECT session_id, id FROM web_events EXCEPT ALL SELECT session_id, id FROM web_events WHERE page = '/search'

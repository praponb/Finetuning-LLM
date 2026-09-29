SELECT id, duration_sec, event_time FROM web_events WHERE session_id = 25 AND event_type <> 'page_view' ORDER BY duration_sec ASC

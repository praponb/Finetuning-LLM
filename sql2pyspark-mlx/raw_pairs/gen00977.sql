SELECT event_time, event_type, CONCAT_WS('-', page, event_type) page_event_type_key, LOWER(page) page_lower FROM web_events WHERE session_id = 18

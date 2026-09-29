SELECT id, page, event_type, duration_sec, CONCAT_WS('-', page, event_type) page_event_type_key FROM web_events WHERE duration_sec < 440 AND (session_id IN (2, 14, 34, 40) OR event_type = 'search')

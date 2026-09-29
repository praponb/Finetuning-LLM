SELECT id, event_type, event_time, page, INITCAP(TRIM(event_type)) AS event_type_title FROM web_events WHERE session_id = 36 ORDER BY id DESC

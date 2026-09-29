SELECT id, page, event_type, LENGTH(page) AS page_length FROM web_events WHERE session_id IN (3, 8, 25, 30)

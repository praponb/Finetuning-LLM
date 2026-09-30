SELECT id, session_id, page, event_time, INITCAP(TRIM(page)) AS page_title FROM web_events WHERE session_id IN (35, 36, 40);

SELECT id, duration_sec, event_time, TRIM(page) AS clean_page, SUBSTRING(event_type, 1, 2) AS event_type_prefix FROM web_events;

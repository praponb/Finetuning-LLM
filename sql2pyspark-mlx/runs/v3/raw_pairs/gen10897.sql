SELECT id, duration_sec, page, event_time, CONCAT_WS(',', page, event_type) AS page_event_type_key FROM web_events

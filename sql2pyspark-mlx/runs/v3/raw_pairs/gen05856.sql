SELECT id, page, event_time, UPPER(event_type) AS event_type_upper, CONCAT_WS('-', page, event_type) AS page_event_type_key FROM web_events ORDER BY id DESC

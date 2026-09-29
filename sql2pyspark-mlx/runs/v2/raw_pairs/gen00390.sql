SELECT *, CONCAT_WS(',', page, event_type) AS page_event_type_key, CONCAT_WS(',', page, event_type) AS page_event_type_key_2 FROM web_events ORDER BY session_id ASC;

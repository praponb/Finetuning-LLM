SELECT id, event_type, duration_sec, UPPER(event_type) AS event_type_upper FROM web_events WHERE session_id IN (5, 14, 16, 26) AND event_type NOT IN ('page_view', 'click') AND duration_sec <> 145;

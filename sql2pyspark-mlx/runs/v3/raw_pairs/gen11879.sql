SELECT event_type, duration_sec, session_id, LENGTH(event_type) AS event_type_length, CASE WHEN page = '/search' THEN 1 ELSE 0 END AS is_search FROM web_events

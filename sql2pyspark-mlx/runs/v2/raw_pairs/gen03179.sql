SELECT duration_sec, event_time, event_type, SUBSTRING(event_type, 1, 3) AS event_type_prefix, CASE WHEN page = '/checkout' THEN 1 ELSE 0 END AS is_checkout FROM web_events WHERE duration_sec = 304;

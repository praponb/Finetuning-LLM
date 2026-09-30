SELECT session_id, event_type, CONCAT(page, ', ', event_type) AS page_event_type FROM web_events WHERE duration_sec > 223 OR TO_DATE(event_time) < '2024-08-01';

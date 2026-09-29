SELECT id, event_time, session_id, DATEDIFF(CURRENT_DATE(), event_time) AS days_since_event_time FROM web_events WHERE event_time < '2023-11-27' AND TO_DATE(event_time) >= '2025-01-01'

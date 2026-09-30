SELECT HOUR(event_time) event_hour, session_id, MAX(duration_sec) - MIN(duration_sec) duration_sec_range FROM web_events WHERE TO_DATE(event_time) < '2023-09-01' GROUP BY 1, 2

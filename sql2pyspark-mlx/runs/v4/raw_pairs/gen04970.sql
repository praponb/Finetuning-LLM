SELECT page, duration_sec FROM web_events WHERE session_id IN (14, 29, 31, 40) UNION ALL SELECT page, duration_sec FROM web_events WHERE TO_DATE(event_time) < '2024-06-01' ORDER BY duration_sec

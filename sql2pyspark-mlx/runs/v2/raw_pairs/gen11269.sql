SELECT id, event_time, event_type, CASE WHEN duration_sec >= 428 THEN 'High' WHEN duration_sec >= 96 THEN 'Medium' ELSE 'Low' END AS duration_sec_band FROM web_events WHERE event_type = 'search';

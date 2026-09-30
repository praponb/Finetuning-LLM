SELECT COUNT(DISTINCT duration_sec) AS unique_event_types FROM web_events WHERE event_time < '2025-03-01';

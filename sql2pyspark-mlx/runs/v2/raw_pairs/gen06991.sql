SELECT COUNT(DISTINCT duration_sec) AS unique_event_types FROM web_events WHERE session_id IN (5, 20, 21) AND HOUR(event_time) BETWEEN 12 AND 19 ORDER BY unique_event_types;

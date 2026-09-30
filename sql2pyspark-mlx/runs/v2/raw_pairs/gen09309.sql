SELECT COUNT(DISTINCT duration_sec) AS unique_sessions, MIN(event_time) AS first_event_time, SUM(duration_sec) AS total_duration_sec FROM web_events;

SELECT event_type, MIN(event_time) AS first_event_time, ROUND(AVG(duration_sec), 2) AS avg_duration_sec, MAX(duration_sec) AS max_duration_sec FROM web_events GROUP BY event_type;

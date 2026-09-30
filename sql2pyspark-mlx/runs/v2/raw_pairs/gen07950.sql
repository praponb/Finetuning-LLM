SELECT id, event_type, duration_sec, SUM(duration_sec) OVER (PARTITION BY event_type) AS event_type_sum_duration_sec FROM web_events;

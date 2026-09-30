SELECT id, event_time, session_id, duration_sec, LEAD(duration_sec, 1) OVER (PARTITION BY DATE_TRUNC('YEAR', event_time), session_id ORDER BY event_time, id) AS next_duration_sec FROM web_events;

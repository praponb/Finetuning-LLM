SELECT id, event_type, event_time, COALESCE(duration_sec, 0) AS duration_sec_filled, ROUND(duration_sec, 1) AS duration_sec_rounded FROM web_events;

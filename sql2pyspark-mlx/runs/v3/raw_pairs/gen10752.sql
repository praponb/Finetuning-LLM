SELECT id, event_type, CAST(event_time AS DATE) AS event_time_day FROM web_events WHERE session_id IN (13, 24, 25) AND (duration_sec IS NOT NULL OR duration_sec > 215)

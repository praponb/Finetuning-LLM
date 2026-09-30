SELECT id, event_type, DATEDIFF(CURRENT_DATE(), event_time) / 365.25 AS years_since_event_time, CEIL(duration_sec) AS duration_sec_whole FROM web_events WHERE duration_sec <> 129;

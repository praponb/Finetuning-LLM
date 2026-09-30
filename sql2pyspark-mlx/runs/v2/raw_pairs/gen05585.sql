SELECT id, event_time, duration_sec, page, TO_DATE(event_time) AS event_time_date FROM web_events

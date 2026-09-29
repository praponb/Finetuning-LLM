SELECT *, TO_DATE(event_time) AS event_time_date, COALESCE(duration_sec, 0) AS duration_sec_filled FROM web_events;
